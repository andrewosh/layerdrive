var fs = require('fs')
var os = require('os')
var p = require('path')
var events = require('events')
var stream = require('stream')
var inherits = require('inherits')

var tar = require('tar-fs')
var level = require('level')
var lexint = require('lexicographic-integer')
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

var DB_FILE = '/.layerdrive.db.tar'
var DB_TEMP_FILE = 'DB'
var JSON_FILE = '/.layerdrive.json'
var HEAD_KEY = 'HEAD'

function Layerdrive (key, opts) {
  if (!(this instanceof Layerdrive)) return new Layerdrive(key, opts)
  if (!key) return new Error('Layerdrives must specify a metadata archive key.')
  opts = opts || {}
  events.EventEmitter.call(this)

  this.opts = opts
  this.key = key
  this.driveStorage = null
  this.baseDrive = null
  this.metadataDrive = null
  this.layerDrive = null

  this.storage = opts.layerStorage
  this.cow = null

  // TODO(andrewosh): more flexible indexing
  this.fileIndex = null
  this.metadata = null
  this.layers = null
  this.layerDrives = {}

  this.driveFactory = opts.driveFactory || Hyperdrive

  this.ready = thunky(open.bind(this))
  this.ready(onready)

  const self = this

  function onready (err) {
    if (err) return self.emit('error', err)
    return self.emit('ready')
  }

  function open (cb) {

    self.driveStorage = getLayerStorage(self.storage, HEAD_KEY)
    self.baseDrive = self.driveFactory(self.driveStorage, self.key, self.opts)
    self.layerDrive = self.driveFactory(self.driveStorage, self.opts)
    self.metadataDrive = self.driveFactory(self.driveStorage, self.opts)

    self.baseDrive.on('content', function () {
      self.layerDrive.ready(function (err) {
        self.metadataDrive.ready(function (err) {
          if (err) return cb(err)
          self.key = self.drive.key
          createTempStorage()
        })
      })
    })

    function createTempStorage () {
      getTempStorage(opts.tempStorage, function (err, tempStorage) {
        if (err) return cb(err)
        self.cow = tempStorage
        processMetadata()
      })
    }

    function processMetadata () {
      if (version) opts.version = version
      var keyString = makeKeyString(key)
      self._readMetadata(loadLayers)
    }

    function loadLayers (err) {
      if (err) return cb(err)
      if (!self.layers) return cb(null)
      self.layers.forEach(function (layer) {
        var layerStorage = getLayerStorage(self.storage, makeKeyString(layer.key))
        var drive = self.driveFactory(layerStorage, layer.key, self.opts)
        self.layerDrives[layer.key] = drive
      })
      return cb(null)
    }
  }
}

inherits(Layerdrive, events.EventEmitter)

Layerdrive.prototype._readMetadata = function (cb) {
  var self = this
  self.baseDrive.ready(function (err) {
    if (err) return cb(err)
    loadDatabase()
  })

  function readDatabase () {
    var indexPath = p.join(self.cow.base, DB_TEMP_FILE)
    self.baseDrive.exists(DB_FILE, function (exists) {
      if (!exists) {
        self.fileIndex = level(indexPath)
        return cb(null)
      }
      var dbStream = self.baseDrive.createReadStream(DB_FILE)
      // The database is copied to the new metadata archive's DB_FILE on commit.
      pump(tarStream, tar.extract(indexPath), function (err) {
        if (err) return cb(err)
        self.fileIndex = level(indexPath)
        loadJson()
      })
    })
  }

  function readJson () {
    self.baseDrive.exists(JSON_FILE, function (exists) {
      if (!exists) return cb(new Error('Malformed metadata drive: db without json'))
      self.baseDrive.readFile(JSON_FILE, 'utf-8', function (err, contents) {
        if (err) return cb(err)
        try {
          var jsonContents = JSON.parse(contents)
          self.metadata = jsonContents
          self.layers = self.metadata.layers
          return cb()
        } catch (err) {
          if (err) return cb(err)
        }
    })
  })
}

Layerdrive.prototype._writeMetadata = function (cb) {
  var self = this
  self.metadataDrive.ready(function (err) {
    if (err) return cb(err)
    writeDatabase()
  })

  function writeDatabase() {
    if (err) return cb(err)
    pump(tar.pack(p.join(self.cow.base, DB_TEMP_FILE)),
        self.metadataDrive.createWriteStream(DB_FILE), function (err) {
          if (err) return cb(err)
          return writeJson()
    })
  }

  function writeJson () {
    var metadata = {
      key: makeKeyString(self.metadataDrive.key),
      version: self.layerDrive.version,
      layers: self.layers
    }
    if (self.opts.name) metadata.name = self.opts.name
    self.metadataDrive.writeFile(JSON_FILE, JSON.stringify(metadata), 'utf-8', cb)
  }
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

Layerdrive.prototype._updateModifier = function (name, cb) {
  var buf = new Buffer(1)
  buf.writeUint8(self.layers.length)
  this.fileIndex.put(toIndexKey(name), buf, cb)
}

Layerdrive.prototype._batchUpdateModifiers = function (files, cb) {
  var batch = files.map(function (file) {
    var buf = new Buffer(1)
    buf.writeUint8(self.layers.length)
    return { type: 'put', key: toIndexKey(file), value: buf }
  })
  this.fileIndex.batch(batch,  cb)
}


Layerdrive.prototype.commit = function (cb) {
  var self = this
  this.layers.push({ key: self.layerDrive.key, version: self.layerDrive.version })
  this.ready(function (err) {
    if (err) return cb(err)
    var files = []
    var status = hyperImport(self.layerDrive, self.cow.base, { ignore: DB_TEMP_FILE },
      function (err) {
        if (err) return cb(err)
        // TODO(andrewosh): improve archive storage for non-directory storage types.
        // The resulting Hyperdrive should be stored in a properly-named directory.
        self._batchUpdateModifiers(files, writeMetadata)
        function writeMetadata (err) {
          if (err) return cb(err)
          if (typeof self.driveStorage === 'string') {
            var newStorage = getLayerStorage(self.storage, makeKeyString(self.layerDrive.key))
            fs.rename(self.driveStorage, newStorage, function (err) {
              if (err) return cb(err)
              return self._writeMetadata(cb)
            })
          } else {
            return self._writeMetadata(cb)
          }
        }
    })
    status.on('file imported', function (entry) {
      var relativePath = p.relative(self.cow.base, entry.path)
      files.push(relativePath)
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

function toIndexKey (name) {
  var depth = name.split('/').length - 1
  return lexint.pack(depth, 'hex') + name
}

function fromIndexKey (key) {
  return key.slice(2)
}

process.on('exit', function () {
  temp.cleanupSync()
})
