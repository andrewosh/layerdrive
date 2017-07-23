var p = require('path')
var events = require('events')
var stream = require('stream')

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

function Layerdrive (key, opts) {
  if (!(this instanceof Layerdrive)) return new Layerdrive(key, opts)
  if (!key) return new Error('Layerdrives must specify base archive keys.')
  opts = opts || {}

  this.opts = opts
  this.key = key
  this.version = opts.version
  this.storage = opts.layerStorage
  this.cow = getTempStorage(opts.tempStorage)

  this.parentKey = null
  this.parentVersion = null

  // TODO(andrewosh): more flexible indexing
  this.lastModifiers = {}
  this.modsByLayer = {}
  this.modsByLayer[HEAD_KEY] = []
  this.layers = []
  this.baseLayer = null

  this.ready = thunky(open)
  this.ready(onready)

  var self = this

  function onready (err) {
    if (err) return this.emit('error', err)
    return this.emit('ready')
  }

  function open (cb) {
    getTempStorage(opts.tempStorage, function (err, tempStorage) {
      if (err) return cb(err)
      self.cow = tempStorage
      processLayer(self.key, self.version)
    })

    function processLayer (key, version) { 
      if (version) opts.version = version
      var layerStorage = getLayerStorage(self.storage, key)
      var layer = Hyperdrive(layerStorage, key, opts)

      self.modsByLayer[key] = []
      self.layers[key] = layer

      currentLayer.on('ready', function () {
        self._readMetadata(currentLayer, function (err, meta) {
          if (err) return cb(err)
          if (!meta) {
            self.baseLayer = key
            return cb(null)
          }
          for (var file in meta.modifiedFiles) {
            self.lastModifiers[mod] = key
            self.modsByLayer[key][file] = true
          }
          processLayer(meta.key, meta.version)
        })
      })
    }
  }
}

inherits(Layerdrive, events.EventEmitter)

Layerdrive.prototype._readMetadata = function (layer, cb) {
  layer.ready(function (err) {
    if (err) return cb(err)
    layer.readFile('/.layerdrive.json', 'utf-8', function (err, contents) {
      if (err) return cb(err)
      try {
        return cb(null, JSON.parse(contents))
      } catch (err) {
        return cb(err)
      }
    })
  })
}

Layerdrive.prototype._writeMetadata = function (layer, cb) {
  var self = this
  layer.ready(function (err) {
    if (err) return cb(err)
    var metadata = JSON.stringify({
      parent: self.key,
      version: self.version,
      modifiedFiles: Object.keys(self.modsByLayer[HEAD_KEY])
    })
    layer.writeFile('./layerdrive.json', 'utf-8', metadata, cb)
  })
}

Layerdrive.prototype._getReadLayer = function (name) {
  var lastModifier = self.lastModifiers[name]
  var readLayer = lastModifier ? self.layers[lastModifier] : self.baseLayer
}

Layerdrive.prototype._copyOnWrite (readLayer, name, cb) {
  return pump(readLayer.createReadStream(name), this.cow.createWriteStream(name), cb)
}

Layerdrive.prototype._updateModifier (name) {
  this.lastModifiers[name] = HEAD_KEY
  this.modsByLayer[HEAD_KEY][name] = true
}

Layerdrive.prototype.commit = function (cb) {
  var self = this
  var driveStorage = this.getLayerStorage(this.storage, HEAD_KEY)
  var drive = Hyperdrive(driveStorage, this.opts)
  this._writeMetadata(drive, function (err) {
    if (err) return cb(err)
    hyperImport(drive, self.cow.base, function (err) {
      if (err) return cb(err)
      // TODO(andrewosh): improve archive storage for non-directory storage types.
      // The resulting Hyperdrive should be stored in a properly-named directory.
      if (typeof driveStorage === 'string') {
        fs.rename(driveStorage, this.getLayerStorage(this.storage, drive.key), cb)
      } else {
        return cb(null)
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
  var readStream = new stream.Readable()
  this.ready(function (err) {
    if (err) return readStream.emit('error', err)
    var readLayer = self._getReadLayer(name)
    pump(readLayer.createReadStream(name, opts), readStream)
  })
  return readStream
}

Layerdrive.prototype.readFile = function (name, opts, cb) {
  this.ready(function (err) {
    if (err) return cb(err)
    var readLayer = self._getReadLayer(name)
    return readLayer.readFile(name, opts, cb)
  })
}

Layerdrive.prototype.createWriteStream = function (name, opts) {
  var self = this
  var writeStream = new stream.Writable()
  this.ready(function (err) {
    if (err) return writeStream.emit('error', err)
    var readLayer = self._getReadLayer(name)
    if (readLayer != self.key) {
      self._copyOnWrite(readLayer, name, oncopy)
    } else {
      oncopy()
    }
    function oncopy (err) {
      if (err) return writeStream.emit('error', err)
      pump(writeStream, self.cow.createWriteStream(name, opts), function (err) {
        if (err) return writeStream.emit('error', err)
        self._updateModifier(name)
      })
    }
  })
  return writeStream
}

Layerdrive.prototype.writeFile = function (name, buf, opts, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    var stream self.createWriteStream(name, opts)
    stream.write(buf)
    stream.on('end', cb)
    stream.on('error', cb)
  })
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
    temp.mkdir({
      prefix: "container-",
      dir: p.join(os.tmpDir(), "containers")
    }, function (err, dir) {
      if (err) return cb(err)
      return cb(null, new ScopedFs(dir))
    })
  } else {
    return cb(null, storage)
  }
}
