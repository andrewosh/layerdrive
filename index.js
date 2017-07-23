var p = require('path')
var events = require('events')
var thunky  = require('thunky')
var from = require('from2')
var temp = require('temp')
var hyperImport = require('hyperdrive-import-files')
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
  this.lastModifier = {}
  this.modsByLayer = {}
  this.modsByLayer[HEAD_KEY] = []
  this.layers = []

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
      if (version) opts.version = nextVersion
      var layerStorage = getLayerStorage(self.storage, key)
      var layer = Hyperdrive(layerStorage, key, opts)

      modsByLayer[nextKey] = []
      layers.push(layer)

      currentLayer.on('ready', function () {
        self._readMetadata(currentLayer, function (err, meta) {
          if (err) return cb(err)
          if (!meta) return cb(null)
          for (var file in meta.modifiedFiles) {
            self.lastModifier[mod] = layer
            self.modsByLayer[nextKey].push(file)
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
      modifiedFiles: self.modsByLayer[HEAD_KEY]
    })
    layer.writeFile('./layerdrive.json', 'utf-8', metadata, cb)
  })
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

}

Layerdrive.prototype.createReadStream = function (name, opts) {
}

Layerdrive.prototype.readFile = function (name, opts, cb) {

}

Layerdrive.prototype.createWriteStream = function (name, opts) {
  this.mods[name] = true
}

Layerdrive.prototype.writeFile = function (name, buf, opts, cb) {
  this.mods[name] = true

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
