var fs = require('fs')
var os = require('os')
var p = require('path')
var events = require('events')
var stream = require('stream')
var inherits = require('inherits')

var duplexify = require('duplexify')
var mkdirp = require('mkdirp')
var pump = require('pump')
var thunky = require('thunky')
var temp = require('temp')
var hyperImport = require('hyperdrive-import-files')
var mux = require('multiplex')
var ScopedFs = require('scoped-fs')
var Hyperdrive = require('hyperdrive')

module.exports = Layerdrive

var HEAD_KEY = 'HEAD'

function Layerdrive (parent, opts) {
  if (!(this instanceof Layerdrive)) return new Layerdrive(parent, opts)
  if (!parent) return new Error('Layerdrives must specify parent archive key.')
  opts = opts || {}
  events.EventEmitter.call(this)

  this.opts = opts
  this.parent = parent
  this.version = opts.version
  this.driveStorage = null
  this.drive = null
  this.key = null

  this.storage = opts.layerStorage
  this.cow = null

  // TODO(andrewosh): more flexible indexing
  this.lastModifiers = {}
  this.modsByLayer = {}
  this.modsByLayer[HEAD_KEY] = []
  this.layers = []
  this.baseLayer = null

  this.driveFactory = opts.driveFactory || Hyperdrive

  const self = this

  this.ready = thunky(open.bind(this))
  this.ready(onready)

  function onready (err) {
    if (err) return self.emit('error', err)
    return self.emit('ready')
  }

  function open (cb) {
    var metadata = []

    self.driveStorage = getLayerStorage(self.storage, HEAD_KEY)
    self.drive = self.driveFactory(self.driveStorage, this.opts)
    self.drive.ready(function (err) {
      if (err) return cb(err)
      self.key = self.drive.key
      createTempStorage()
    })

    function createTempStorage () {
      getTempStorage(opts.tempStorage, function (err, tempStorage) {
        if (err) return cb(err)
        self.cow = tempStorage
        processLayer(self.parent, self.version)
      })
    }

    function processLayer (key, version) {
      if (version) opts.version = version
      var keyString = makeKeyString(key)
      var layerStorage = getLayerStorage(self.storage, keyString)
      var layer = self.driveFactory(layerStorage, key, opts)

      self.modsByLayer[key] = []
      self.layers[key] = layer

      layer.on('content', thunky(function () {
        self.emit('content')
        self._readMetadata(layer, function (err, meta) {
          if (err) return cb(err)
          metadata.unshift({ key: key, meta: meta })
          if (!meta) {
            self.baseLayer = key
            return indexLayers(metadata, cb)
          }
          processLayer(Buffer.from(meta.parent, 'hex'), meta.version)
        })
      }))
    }

    function indexLayers (metadata, cb) {
      metadata.forEach(function (entry) {
        if (!entry.meta) return
        entry.meta.modifiedFiles.forEach(function (file) {
          self.lastModifiers[file] = entry.key
          self.modsByLayer[entry.key][file] = true
        })
      })
      return cb(null)
    }
  }
}

inherits(Layerdrive, events.EventEmitter)

Layerdrive.prototype.getHyperdrives = function (cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    return self.layers
  })
}

Layerdrive.prototype._readMetadata = function (layer, cb) {
  layer.ready(function (err) {
    if (err) return cb(err)
    layer.exists('/.layerdrive.json', function (exists) {
      if (!exists) return cb(null)
      layer.readFile('/.layerdrive.json', 'utf-8', function (err, contents) {
        if (err) return cb(err)
        try {
          var metadata = JSON.parse(contents)
          return cb(null, metadata)
        } catch (err) {
          return cb(err)
        }
      })
    })
  })
}

Layerdrive.prototype._writeMetadata = function (cb) {
  var self = this
  self.drive.ready(function (err) {
    if (err) return cb(err)
    var metadata = JSON.stringify({
      parent: makeKeyString(self.parent),
      version: self.version,
      modifiedFiles: Object.keys(self.modsByLayer[HEAD_KEY])
    })
    self.drive.writeFile('/.layerdrive.json', metadata, 'utf-8', cb)
  })
}

Layerdrive.prototype._getReadLayer = function (name) {
  var self = this
  var lastModifier = self.lastModifiers[name]
  if (lastModifier === HEAD_KEY) {
    return self.cow
  }
  var readLayer = lastModifier ? self.layers[lastModifier] : self.layers[self.baseLayer]
  return readLayer
}

Layerdrive.prototype._copyOnWrite = function (readLayer, name, cb) {
  var self = this
  if (this.lastModifiers[name] === HEAD_KEY) return cb()
  readLayer.exists(name, function (exists) {
    if (!exists) return cb()
    var readStream = readLayer.createReadStream(name)
    var writeStream = self.cow.createWriteStream(name, { start: 0 })
    return pump(readStream, writeStream, function (err) {
      cb(err)
    })
  })
}

Layerdrive.prototype._updateModifier = function (name) {
  this.lastModifiers[name] = HEAD_KEY
  this.modsByLayer[HEAD_KEY][name] = true
}

Layerdrive.prototype.commit = function (cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    var status = hyperImport(self.drive, self.cow.base, function (err) {
      if (err) return cb(err)
      // TODO(andrewosh): improve archive storage for non-directory storage types.
      // The resulting Hyperdrive should be stored in a properly-named directory.
      if (typeof self.driveStorage === 'string') {
        var newStorage = getLayerStorage(self.storage, makeKeyString(self.drive.key))
        fs.rename(self.driveStorage, newStorage, function (err) {
          if (err) return cb(err)
          return self._writeMetadata(cb)
        })
      } else {
        return self._writeMetadata(cb)
      }
    })
    status.on('file imported', function (entry) {
      var relativePath = p.relative(self.cow.base, entry.path)
      return self._updateModifier(relativePath)
    })
  })
}

Layerdrive.prototype.replicate = function (opts) {
  var multistream = mux()
  for (var key in this.layers) {
    var layer = this.layers[key]
    var layerStream = layer.replicate()
    layerStream.pipe(multistream.createStream(key)).pipe(layerStream)
  }
  return multistream
}

Layerdrive.prototype.createReadStream = function (name, opts) {
  var self = this
  var readStream = new stream.PassThrough(opts)
  this.ready(function (err) {
    if (err) return readStream.emit('error', err)
    var readLayer = self._getReadLayer(name)
    pump(readLayer.createReadStream(name, opts), readStream)
  })
  return readStream
}

Layerdrive.prototype.readFile = function (name, opts, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    var readLayer = self._getReadLayer(name)
    readLayer.readFile(name, opts, function (err, contents) {
      return cb(err, contents)
    })
  })
}

Layerdrive.prototype.createWriteStream = function (name, opts) {
  var self = this
  var proxy = duplexify()
  proxy.cork()
  this.ready(function (err) {
    if (err) return proxy.emit('error', err)

    var readLayer = self._getReadLayer(name)
    if (readLayer) {
      self._copyOnWrite(readLayer, name, oncopy)
    } else {
      oncopy()
    }

    function oncopy (err) {
      if (err) return proxy.emit('error', err)
      proxy.setWritable(self.cow.createWriteStream(name, opts))
      proxy.setReadable(false)
      proxy.uncork()
      proxy.on('finish', function () {
        self._updateModifier(name)
      })
    }
  })
  return proxy
}

Layerdrive.prototype.writeFile = function (name, buf, opts, cb) {
  if (typeof opts === 'function') return this.writeFile(name, buf, null, opts)
  if (typeof opts === 'string') opts = {encoding: opts}
  if (!opts) opts = {}
  if (typeof buf === 'string') buf = Buffer.from(buf, opts.encoding || 'utf-8')

  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    var stream = self.createWriteStream(name, opts)
    stream.on('error', cb)
    stream.on('finish', cb)
    stream.write(buf)
    stream.end()
  })
}

function makeKeyString (key) {
  return key.toString('hex')
}

function getLayerStorage (storage, layer) {
  storage = storage || p.join(__dirname, 'layers')
  if (typeof storage === 'string') {
    return p.join(storage, makeKeyString(layer))
  }
  return function (name) {
    return storage(p.join(makeKeyString(layer), name))
  }
}

function getTempStorage (storage, cb) {
  if (!storage) {
    temp.track()
    var tempDir = p.join(os.tmpdir(), 'containers')
    mkdirp(tempDir, function (err) {
      if (err) return cb(err)
      temp.mkdir({
        prefix: 'container-',
        dir: p.join(os.tmpdir(), 'containers')
      }, function (err, dir) {
        if (err) return cb(err)
        return cb(null, new ScopedFs(dir))
      })
    })
  } else {
    return cb(null, storage)
  }
}

process.on('exit', function () {
  temp.cleanupSync()
})
