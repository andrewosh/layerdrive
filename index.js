var fs = require('fs')
var os = require('os')
var p = require('path')
var events = require('events')
var stream = require('stream')
var inherits = require('inherits')

var collect = require('stream-collector')
var cuid = require('cuid')
var tar = require('tar-stream')
var tarFs = require('tar-fs')
var level = require('level')
var lexint = require('lexicographic-integer')
var duplexify = require('duplexify')
var mkdirp = require('mkdirp')
var pump = require('pump')
var thunky = require('thunky')
var hyperImport = require('hyperdrive-import-files')
var mux = require('multiplex')
var spy = require('through2-spy')
var ScopedFs = require('scoped-fs')

var log = require('debug')('layerdrive')

// TODO: merge symlink/link/chmod/chown upstream?
var Stat = require('hyperdrive/lib/stat')
var DEFAULT_FMODE = (4 | 2 | 0) << 6 | ((4 | 0 | 0) << 3) | (4 | 0 | 0) // rw-r--r--

var temp = require('temp')
temp.track()

module.exports = Layerdrive

var DEFAULT_LAYER_DIR = './layers'
var DB_FILE = '/.layerdrive.db.tar'
var DB_TEMP_FILE = 'DB'
var JSON_FILE = '/.layerdrive.json'

function Layerdrive (key, driveFactory, opts) {
  if (!(this instanceof Layerdrive)) return new Layerdrive(key, driveFactory, opts)
  if (!key) return new Error('Layerdrives must specify a metadata archive key.')
  opts = opts || {}
  events.EventEmitter.call(this)

  this.opts = opts
  this.key = key
  this.driveStorage = null
  this.baseDrive = null
  this.metadataDrive = null
  this.layerDrive = null

  this.layerDir = opts.layerDir || DEFAULT_LAYER_DIR
  this.cow = null

  // TODO(andrewosh): more flexible indexing
  this.fileIndex = null
  this.metadata = null
  this.layers = []
  this.layerDrives = {}

  this.statCache = {}

  var self = this
  this.driveFactory = driveFactory
  this.createHyperdrive = function (key, opts) {
    if ((typeof key === 'object') && !(key instanceof Buffer)) {
      opts = key
      key = null
    }
    var id = key ? toKeyString(key) : cuid()
    var storage = p.join(self.layerDir, id)
    return self.driveFactory(storage, key, opts)
  }

  this.ready = thunky(open)
  this.ready(onready)

  function onready (err) {
    if (err) return self.emit('error', err)
    return self.emit('ready')
  }

  function open (cb) {
    if (typeof self.key === 'string') {
      return self._createBaseLayerdrive(function (err) {
        if (err) return cb(err)
        return _open(null, cb)
      })
    }
    return _open(null, cb)
  }

  function _open (err, cb) {
    if (err) return cb(err)
    self.baseDrive = self.createHyperdrive(self.key, self.opts)
    self.layerDrive = self.createHyperdrive()
    self.metadataDrive = self.createHyperdrive()

    self.baseDrive.on('ready', function () {
      self.layerDrive.ready(function (err) {
        if (err) return cb(err)
        self.metadataDrive.ready(function (err) {
          if (err) return cb(err)
          self.key = self.metadataDrive.key
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
      self._readMetadata(loadLayers)
    }

    function loadLayers (err) {
      if (err) return cb(err)
      if (self.layers.length === 0) return cb(null)
      self.layers.forEach(function (layer) {
        var drive = self.createHyperdrive(layer.key, self.opts)
        self.layerDrives[layer.key] = drive
      })
      return cb(null)
    }
  }
}

inherits(Layerdrive, events.EventEmitter)

Layerdrive.prototype._createBaseLayerdrive = function (cb) {
  var self = this
  var layerDrive = self.createHyperdrive(self.opts)
  var metadataDrive = self.createHyperdrive(self.opts)
  temp.mkdir({
    prefix: 'db'
  }, function (err, dir) {
    if (err) return cb(err)
    var db = level(dir, { valueEncoding: 'binary' })
    metadataDrive.on('ready', function () {
      var layers = [{ key: layerDrive.key, version: layerDrive.version }]
      var extract = pump(fs.createReadStream(self.key), tar.extract(), function (err) {
        if (err) return cb(err)
      })
      extract.on('entry', function (header, stream, next) {
        var key = Buffer.alloc(1)
        key.writeUInt8(0)
        if (header.name.startsWith('.')) header.name = header.name.slice(1)
        db.put(toIndexKey(header.name), key, function (err) {
          if (err) return cb(err)
          if (header.name.endsWith('/')) {
            layerDrive.mkdir(header.name, next)
          } else {
            pump(stream, layerDrive.createWriteStream(header.name), function (err) {
              if (err) return cb(err)
              return next()
            })
          }
        })
      })
      extract.on('finish', function () {
        var rootKey = Buffer.alloc(1)
        rootKey.writeUInt8(0)
        db.put(toIndexKey('/'), rootKey, function (err) {
          if (err) return cb(err)
          pump(tarFs.pack(dir), metadataDrive.createWriteStream(DB_FILE), function (err) {
            if (err) return cb(err)
            metadataDrive.writeFile(JSON_FILE, JSON.stringify({
              layers: layers
            }), { encoding: 'utf-8' }, function (err) {
              if (err) return cb(err)
              self.key = metadataDrive.key
              return cb()
            })
          })
        })
      })
    })
  })
}

Layerdrive.prototype._readMetadata = function (cb) {
  var self = this
  self.baseDrive.ready(function (err) {
    if (err) return cb(err)
    readDatabase()
  })

  function readDatabase () {
    self.baseDrive.readdir('/', function (err, dir) {
      if (err) return cb(err)
      readdb()
    })
    function readdb () {
      var indexPath = p.join(self.cow.base, DB_TEMP_FILE)
      var dbStream = self.baseDrive.createReadStream(DB_FILE)
      // The database is copied to the new metadata archive's DB_FILE on commit.
      pump(dbStream, tarFs.extract(indexPath), function (err) {
        if (err) return cb(err)
        self.fileIndex = level(indexPath, { valueEncoding: 'binary' })
        readJson()
      })
    }
  }

  function readJson (err) {
    if (err) return cb(err)
    self.baseDrive.exists(JSON_FILE, function (exists) {
      if (!exists) return cb(new Error('Malformed metadata drive: db without json'))
      self.baseDrive.readFile(JSON_FILE, 'utf-8', function (err, contents) {
        if (err) return cb(err)
        try {
          var jsonContents = JSON.parse(contents)
          self.metadata = jsonContents
          self.layers = self.metadata.layers.map(function (entry) {
            return { key: fromKeyString(entry.key), version: entry.version }
          })
          return cb()
        } catch (err) {
          if (err) return cb(err)
        }
      })
    })
  }
}

Layerdrive.prototype._updateStat = function (driveStat, metaStat) {
  if (metaStat.uid && (metaStat.uid !== driveStat.uid)) { driveStat.uid = metaStat.uid }
  if (metaStat.gid && (metaStat.gid !== driveStat.gid)) { driveStat.gid = metaStat.gid }
  if (metaStat.mode && (metaStat.mode !== driveStat.mode)) { driveStat.mode = metaStat.mode }
  if (metaStat.linkname && (metaStat.linkname !== driveStat.linkname)) { driveStat.linkname = metaStat.linkname }
}

Layerdrive.prototype._writeUpdatedStats = function (cb) {
  var self = this
  self.layerDrive.ready(function (err) {
    if (err) return cb(err)
    var stats = Object.keys(self.statCache)
    var name = null

    if (stats.length !== 0) return writeNextStat()
    return cb()

    function writeNextStat () {
      name = stats.pop()
      self.layerDrive.readdir('/', function (err, list) {
        if (err) return cb(err)
        self.layerDrive.stat(name, function (err, driveStat) {
          if (err) return cb(err)
          var metaStat = self.statCache[name]
          self._updateStat(driveStat, metaStat)
          self.layerDrive.tree.put(name, driveStat, function (err) {
            if (err) return cb(err)
            self.layerDrive.stat(name, function (err, stat) {
              if (err) return cb(err)
            })
            if (stats.length !== 0) return writeNextStat()
            return cb()
          })
        })
      })
    }
  })
}

Layerdrive.prototype._writeMetadata = function (cb) {
  var self = this
  self.metadataDrive.ready(function (err) {
    if (err) return cb(err)
    writeDatabase()
  })

  function writeDatabase () {
    self.fileIndex.close(function (err) {
      if (err) return cb(err)
      pump(tarFs.pack(p.join(self.cow.base, DB_TEMP_FILE)),
          self.metadataDrive.createWriteStream(DB_FILE), function (err) {
            if (err) return cb(err)
            return writeJson()
          }
      )
    })
  }

  function writeJson () {
    self.layers.push({
      key: self.layerDrive.key,
      version: self.layerDrive.version
    })
    var metadata = {
      layers: self.layers.map(function (entry) {
        return { key: toKeyString(entry.key), version: entry.version }
      })
    }
    if (self.opts.name) metadata.name = self.opts.name
    self.metadataDrive.writeFile(JSON_FILE, JSON.stringify(metadata), 'utf-8', cb)
  }
}

Layerdrive.prototype._getReadLayer = function (name, cb) {
  var self = this
  this.fileIndex.get(toIndexKey(name), function (err, indexBuf) {
    if (err) return cb(err)
    var index = indexBuf.readUInt8()
    var layer = (index === self.layers.length) ? self.cow
      : self.layerDrives[self.layers[index].key]
    return cb(null, layer, index)
  })
}

Layerdrive.prototype._copyOnWrite = function (readLayer, src, dest, cb) {
  if (typeof dest === 'function') return this._copyOnWrite(readLayer, src, null, dest)
  var self = this
  dest = dest || src
  var readStream = readLayer.createReadStream(src)
  var writeStream = self.cow.createWriteStream(dest, { start: 0 })
  return pump(readStream, writeStream, function (err) {
    if (err) return cb(err)
    return cb()
  })
}

Layerdrive.prototype._makeDbValue = function () {
  var buf = Buffer.alloc(1)
  buf.writeUInt8(this.layers.length)
  return buf
}

Layerdrive.prototype._updateFileIndex = function (name, cb) {
  this.fileIndex.put(toIndexKey(name), this._makeDbValue(), cb)
}

Layerdrive.prototype._batchUpdateModifiers = function (files, cb) {
  var self = this
  var batch = files.map(function (file) {
    return { type: 'put', key: toIndexKey(file), value: self._makeDbValue() }
  })
  this.fileIndex.batch(batch, cb)
}

Layerdrive.prototype._findFile = function (name, cb) {
  var self = this
  self.stat(name, function (err, stat, realName) {
    if (err) return cb(err)
    return cb(null, realName)
  })
}

Layerdrive.prototype.commit = function (cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    var files = []
    self.layerDrive.ready(function (err) {
      if (err) return cb(err)
      var status = hyperImport(self.layerDrive, self.cow.base, {
        ignore: p.join(self.cow.base, DB_TEMP_FILE)
      }, function (err) {
        if (err) return cb(err)
        self._batchUpdateModifiers(files, updateDrives)
        function updateDrives (err) {
          if (err) return cb(err)
          self._writeUpdatedStats(function (err) {
            if (err) return cb(err)
            self._writeMetadata(function (err) {
              if (err) return cb(err)
              return cb(null,
                Layerdrive(self.metadataDrive.key, self.driveFactory, self.opts))
            })
          })
        }
      })
      status.on('file imported', function (entry) {
        var relativePath = '/' + p.relative(self.cow.base, entry.path)
        files.push(relativePath)
      })
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
  log('createReadStream', name, opts)
  var self = this
  var readStream = new stream.PassThrough(opts)
  this._findFile(name, function (err, realName) {
    if (err) return readStream.emit('error', err)
    self._getReadLayer(realName, function (err, layer) {
      if (err) return readStream.emit('error', err)
      pump(layer.createReadStream(realName, opts), readStream)
    })
  })
  return readStream
}

Layerdrive.prototype.readFile = function (name, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  opts = opts || {}
  var self = this
  this._findFile(name, function (err, realName) {
    if (err) return cb(err)
    self._getReadLayer(realName, function (err, layer) {
      if (err) return cb(err)
      layer.readFile(realName, opts, function (err, contents) {
        if (err) return cb(err)
        return cb(null, contents)
      })
    })
  })
}

Layerdrive.prototype.createWriteStream = function (name, opts) {
  var self = this
  opts = opts || {}
  var proxy = duplexify()

  var dataLength = 0
  var dataCounter = spy(function (chunk) {
    dataLength += chunk.length
  })
  proxy.cork()

  this.stat(name, function (err, stat, realName) {
    if (err) return proxy.emit('error', err)
    self._getReadLayer(realName, function (err, layer, index) {
      if (err && err.notFound) return onCopy()
      if (err) return proxy.emit('error', err)
      if (layer.key) {
        return self._copyOnWrite(layer, realName, function (err) {
          if (err) return proxy.emit('error', err)
          onCopy(null, layer)
        })
      }
      // The file has already been copied.
      return onCopy(null, layer)
    })
    function onCopy (err, readLayer) {
      if (err) return proxy.emit('error', err)
      if (!stat) {
        stat = {
          nlink: 1,
          uid: 0,
          gid: 0,
          size: 0
        }
      }
      stat.mtime = new Date()
      self._updateFileIndex(realName, function (err) {
        if (err) return proxy.emit('error', err)
        proxy.setWritable(dataCounter)
        proxy.setReadable(false)
        var writeStream = self.cow.createWriteStream(realName, opts)
        dataCounter.pipe(writeStream)
        stat.size = opts.start || 0
        proxy.uncork()
      })
    }
    proxy.on('prefinish', function () {
      stat.size += dataLength
      self.statCache[name] = stat
    })
  })
  return proxy
}

Layerdrive.prototype.writeFile = function (name, buf, opts, cb) {
  if (typeof opts === 'function') return this.writeFile(name, buf, null, opts)
  if (typeof opts === 'string') opts = {encoding: opts}
  if (!opts) opts = {}
  if (typeof buf === 'string') buf = Buffer.from(buf, opts.encoding || 'utf-8')

  var self = this
  this._findFile(name, function (err, realName) {
    if (err) return cb(err)
    var stream = self.createWriteStream(realName, opts)
    stream.on('error', cb)
    stream.on('finish', cb)
    stream.write(buf)
    stream.end()
  })
}

Layerdrive.prototype.append = function (name, buf, opts, cb) {
  if (typeof opts === 'function') return this.append(name, buf, null, opts)
  if (typeof opts === 'string') opts = {encoding: opts}
  if (!opts) opts = {}
  if (typeof buf === 'string') buf = Buffer.from(buf, opts.encoding || 'utf-8')

  var self = this
  this.stat(name, function (err, stat, realName) {
    if (err) return cb(err)
    if (stat) {
      opts.start = stat.size
      opts.flags = 'r+'
    }
    var stream = self.createWriteStream(realName, opts)
    stream.on('error', cb)
    stream.on('finish', cb)
    stream.write(buf)
    stream.end()
  })
}

Layerdrive.prototype.unlink = function (name, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    if (self.statCache[name]) delete self.statCache[name]
    self.fileIndex.del(toIndexKey(name), function (err) {
      if (err) return cb(err)
      self.cow.exists(name, function (exist) {
        if (exist) {
          self.cow.unlink(name, cb)
        } else {
          return cb()
        }
      })
    })
  })
}

Layerdrive.prototype.mkdir = function (name, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  opts = opts || {}
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    self.cow.mkdir(name, opts, function (err) {
      if (err) return cb(err)
      self._updateFileIndex(name, function (err) {
        return cb(err)
      })
    })
  })
}

Layerdrive.prototype.rmdir = function (name, cb) {
  var self = this
  this._findFile(name, function (err, realName) {
    if (err) return cb(err)
    self.readdir(realName, function (err, files) {
      if (err) return cb(err)
      if (files.length > 0) return cb(new Error('Directory is not empty.'))
      self.cow.exists(realName, function (exists) {
        if (exists) return self.cow.rmdir(realName, onremoved)
        return onremoved()
      })
      function onremoved () {
        self._updateFileIndex(realName, cb)
      }
    })
  })
}

Layerdrive.prototype.readdir = function (name, cb) {
  var self = this
  this._findFile(name, function (err, realName) {
    if (err) return cb(err)
    var resolved = p.resolve(realName)
    if (!/\/$/.test(resolved)) resolved += '/'
    var gt = toIndexKey(p.join(resolved, '\x00'))
    var lt = toIndexKey(p.join(resolved, '\xff'))
    var stream = self.fileIndex.createReadStream({ gt: gt, lt: lt })
    collect(stream, function (err, entries) {
      if (err) return cb(err)
      return cb(null, entries.map(function (entry) {
        return fromIndexKey(entry.key)
      }))
    })
  })
}

Layerdrive.prototype.stat = function (name, opts, cb) {
  if (typeof opts === 'function') return this.stat(name, {}, opts)
  log('stat', name)
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    var stat = self.statCache[name]
    self._getReadLayer(name, function (err, layer, index) {
      if (err) return onstat(err)
      if (index === self.layers.length) return onstat()
      return layer.stat(name, onstat)
    })
    function onstat (err, layerStat) {
      if (stat && stat.linkname && !opts.noFollow) return self.stat(stat.linkname, cb)
      if (err && err.notFound) return cb(null, stat, name)
      if (err) return cb(err)
      stat = stat || layerStat
      if (!opts.noFollow) self.statCache[name] = stat
      return cb(null, stat, name)
    }
  })
}

Layerdrive.prototype.mv = function (src, dest, cb) {
  var self = this
  this.stat(src, function (err, stat, realName) {
    if (err) return cb(err)
    self._getReadLayer(realName, function (err, layer, index) {
      if (err) return cb(err)
      self._copyOnWrite(layer, src, dest, function (err) {
        if (err) return cb(err)
        self._updateFileIndex(dest, function (err) {
          if (err) return cb(err)
          self.statCache[dest] = self.statCache[src] 
          self.unlink(src, function (err) {
            return cb(err)
          })
        })
      })
    })
  })
}

Layerdrive.prototype.chown = function (name, uid, gid, cb) {
  var self = this
  this.stat(name, function (err, stat, realName) {
    if (err) return cb(err)
    self._getReadLayer(realName, function (err, layer, index) {
      if (err) return cb(err)
      if (layer.key) return self._copyOnWrite(layer, realName, updateStat)
      return updateStat()
    })
    function updateStat (err) {
      if (err) return cb(err)
      stat.uid = uid
      stat.gid = gid
      return cb(null, stat)
    }
  })
}

Layerdrive.prototype.chmod = function (name, mode, cb) {
  var self = this
  this.stat(name, function (err, stat, realName) {
    if (err) return cb(err)
    self._getReadLayer(realName, function (err, layer, index) {
      if (err) return cb(err)
      if (layer.key) return self._copyOnWrite(layer, realName, updateStat)
      return updateStat()
    })
    function updateStat (err) {
      if (err) return cb(err)
      stat.mode = mode
      return cb(null, stat)
    }
  })
}

Layerdrive.prototype.symlink = function (src, dest, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    self.stat(dest, function (err, stat) {
      if (err) return cb(err)
      if (stat) return cb(new Error('File already exists.'))
      var st = {
        linkname: src,
        nlink: 1,
        uid: 0,
        gid: 0,
        mode: DEFAULT_FMODE | Stat.IFLNK
      }
      self.statCache[dest] = st
      return cb()
    })
  })
}

Layerdrive.prototype.link = function (src, dest, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    self.stat(src, function (err, srcStat) {
      if (err) return cb(err)
      self.stat(dest, function (err, destStat) {
        if (err) return cb(err)
        if (destStat) return cb(new Error('File already exists.'))
        var st = {
          linkname: src,
          uid: 0,
          gid: 0,
          mode: DEFAULT_FMODE | Stat.IFLNK
        }
        self.statCache[dest] = st
        srcStat.nlink++
        return cb()
      })
    })
  })
}

Layerdrive.prototype.mknod = function (path, mode, dev, cb) {
  this.ready(function (err) {
    if (err) return cb(err)
    return cb()
    // TODO: support mknod
  })
}

Layerdrive.prototype.updateStat = function (path, stat, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    self.statCache[path] = stat
    return cb()
  })
}

function toKeyString (key) {
  return key.toString('hex')
}

function fromKeyString (keyString) {
  return Buffer.from(keyString, 'hex')
}

function getTempStorage (storage, cb) {
  if (!storage) {
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
  if (name.endsWith('/')) name = name.slice(0, -1)
  var depth = name.split('/').length - 1
  return lexint.pack(depth, 'hex') + name
}

function fromIndexKey (key) {
  return key.slice(key.indexOf('/'))
}

process.on('exit', function () {
  temp.cleanupSync()
})
