var fs = require('fs')
var os = require('os')
var p = require('path')
var events = require('events')
var stream = require('stream')
var inherits = require('inherits')

var through = require('through2')
var mkdirp = require('mkdirp')
var pump = require('pump')
var thunky  = require('thunky')
var from = require('from2')
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
  this.storage = opts.layerStorage
  this.cow = null

  // TODO(andrewosh): more flexible indexing
  this.lastModifiers = {}
  this.modsByLayer = {}
  this.modsByLayer[HEAD_KEY] = []
  this.layers = []
  this.baseLayer = null

  this.driveFactory = opts.driveFactory || Hyperdrive

  this.ready = thunky(open)
  this.ready(onready)

  var self = this

  function onready (err) {
    if (err) return self.emit('error', err)
    return self.emit('ready')
  }

  function open (cb) {
    var metadata = []

    getTempStorage(opts.tempStorage, function (err, tempStorage) {
      if (err) return cb(err)
      self.cow = tempStorage
      processLayer(self.parent, self.version)
    })

    function processLayer (key, version) { 
      console.log('in processLayer')
      if (version) opts.version = version
      var keyString = makeKeyString(key)
      var layerStorage = getLayerStorage(self.storage, keyString)
      var layer = self.driveFactory(layerStorage, key, opts)
      console.log('here')

      self.modsByLayer[key] = []
      self.layers[key] = layer
      console.log('and here')

      layer.on('ready', function () {
        console.log('oh and here')
        self._readMetadata(layer, function (err, meta) {
          console.log('reading metadata finished')
          if (err) return cb(err)
          metadata.unshift({ key: key, meta: meta })
          if (!meta) {
            console.log('finished processing layers')
            self.baseLayer = key
            return indexLayers(metadata, cb)
          }
          processLayer(new Buffer(meta.parent, 'base64'), meta.version)
        })
      })
    }

    function indexLayers (metadata, cb) {
      console.log('indexing layers...')
      for (var meta in metadata) {
        if (!meta) break;
        for (var file in meta.modifiedFiles) {
          self.lastModifiers[file] = meta.key
          self.modsByLayer[meta.key][file] = true
        }
      }
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
    console.log('the layers ready, err: ', err)
    layer.exists('/.layerdrive.json', function (exists) {
      if (!exists) return cb(null)
      layer.readFile('/.layerdrive.json', 'utf-8', function (err, contents) {
        console.log('read the damn metadata file, err: ', err)
        if (err) return cb(err)
        try {
          return cb(null, JSON.parse(contents))
        } catch (err) {
          return cb(err)
        }
      })
    })
  })
}

Layerdrive.prototype._writeMetadata = function (layer, cb) {
  var self = this
  layer.ready(function (err) {
    if (err) return cb(err)
    var metadata = JSON.stringify({
      parent: self.parent.toString('utf-8'),
      version: self.version,
      modifiedFiles: Object.keys(self.modsByLayer[HEAD_KEY])
    })
    layer.writeFile('/layerdrive.json', metadata, 'utf-8', cb)
  })
}

Layerdrive.prototype._getReadLayer = function (name) {
  var self = this
  var lastModifier = self.lastModifiers[name]
  var readLayer = lastModifier ? self.layers[lastModifier] : self.layers[self.baseLayer]
  return readLayer
}

Layerdrive.prototype._copyOnWrite = function (readLayer, name, cb) {
  if (this.lastModifiers[name] === HEAD_KEY) return cb()
  console.log('copying on write:', name)
  var readStream = readLayer.createReadStream(name)
  var writeStream = this.cow.createWriteStream(name)
  return pump(readStream, writeStream, function (err) {
    cb(err)
  })
}

Layerdrive.prototype._updateModifier = function (name) {
  this.lastModifiers[name] = HEAD_KEY
  this.modsByLayer[HEAD_KEY][name] = true
}

Layerdrive.prototype.commit = function (cb) {
  var self = this
  var driveStorage = getLayerStorage(self.storage, HEAD_KEY)
  var drive = Hyperdrive(driveStorage, this.opts)
  this._writeMetadata(drive, function (err) {
    if (err) return cb(err)
    hyperImport(drive, self.cow.base, function (err) {
      if (err) return cb(err)
      // TODO(andrewosh): improve archive storage for non-directory storage types.
      // The resulting Hyperdrive should be stored in a properly-named directory.
      if (typeof driveStorage === 'string') {
        var newStorage = getLayerStorage(self.storage, makeKeyString(drive.key))
        fs.rename(driveStorage, newStorage, function (err) {
          if (err) return cb(err)
          return cb(null, drive)
        })
      } else {
        return cb(null, drive)
      }
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
    console.log('we\'re ready for business, err:', err)
    if (err) return cb(err)
    var readLayer = self._getReadLayer(name)
    readLayer.on('error', function (err) {console.log('readlayer error:', err)})
    readLayer.readFile(name, opts, function (err, contents) {
      console.error('read error:', err)
      console.log('contents:', contents)
      return cb(null, contents)
    })
  })
}

Layerdrive.prototype.createWriteStream = function (name, opts) {
  var self = this
  var writeStream = new stream.PassThrough(opts)
  this.ready(function (err) {
    if (err) return writeStream.emit('error', err)
    var readLayer = self._getReadLayer(name)
    self._copyOnWrite(readLayer, name, oncopy)
    function oncopy (err) {
      console.log('in oncopy')
      if (err) return writeStream.emit('error', err)
      console.log('at pump')
      pump(writeStream, self.cow.createWriteStream(name, opts), function (err) {
        console.log('after pump')
        if (err) return writeStream.emit('error', err)
        console.log('pump finished')
        self._updateModifier(name)
      })
    }
  })
  return writeStream
}

Layerdrive.prototype.writeFile = function (name, buf, opts, cb) {
  if (typeof opts === 'function') return this.writeFile(name, buf, null, opts)
  if (typeof opts === 'string') opts = {encoding: opts}
  if (!opts) opts = {}
  if (typeof buf === 'string') buf = new Buffer(buf, opts.encoding || 'utf-8')

  var self = this
  this.ready(function (err) {
    console.log('about to create write stream')
    if (err) return cb(err)
    var stream = self.createWriteStream(name, opts)
    stream.write(buf)
    stream.end()
    stream.on('error', cb)
    stream.on('end', cb)
  })
}

function makeKeyString (key) {
  return key.toString('base64')
}

function getLayerStorage(storage, layer) {
  storage = storage || p.join(__dirname, 'layers')
  if (typeof storage === 'string') {
    return p.join(storage, layer)
  }
  return function (name) {
    return storage(p.join(layer, name))
  }
}

function getTempStorage (storage, cb) {
  if (!storage) {
    temp.track()
    var tempDir = p.join(os.tmpdir(), "containers")
    mkdirp(tempDir, function (err) {
      if (err) return cb(err)
      temp.mkdir({
        prefix: "container-",
        dir: p.join(os.tmpdir(), "containers")
      }, function (err, dir) {
        if (err) return cb(err)
        return cb(null, new ScopedFs(dir))
      })
    })
  } else {
    return cb(null, storage)
  }
}
