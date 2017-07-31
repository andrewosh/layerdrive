var p = require('path')

var concat = require('concat-stream')
var test = require('tape')
var pump = require('pump')
var mkdirp = require('mkdirp')
var rimraf = require('rimraf')
var randomstring = require('randomstring')
var randomItem = require('random-item')
var Hyperdrive = require('hyperdrive')

var Layerdrive = require('..')

var TEST_DIR = './test-layers'
mkdirp.sync(TEST_DIR)

var drives = {}

function _applyOps (drive, ops, cb) {
  drive.on('error', function (err) {
    return cb(err)
  })
  drive.ready(onready)

  function onready () {
    var opIndex = 0
    if (ops.length !== 0) _nextOp()
    function _nextOp () {
      var op = ops[opIndex]
      drive.writeFile(op.file, op.contents, function (err) {
        if (err) return cb(err)
        if (++opIndex === ops.length) return cb(null)
        _nextOp()
      })
    }
  }
}

function driveFactory (storage, key, opts) {
  if ((typeof key === 'object') && !(key instanceof Buffer)) {
    opts = key
    key = null
  }
  var drive = Hyperdrive(storage, key, opts)
  drive.on('ready', function () {
    var existingDrive = drives[drive.key]
    if (existingDrive) {
      var existingStream = existingDrive.replicate()
      existingStream.pipe(drive.replicate()).pipe(existingStream)
    } else {
      drives[drive.key] = drive
    }
  })
  return drive
}

function createLayerdrive (base, numLayers, numFiles, opsPerLayer, fileLength, cb) {
  var files = []
  for (var i = 0; i < numFiles; i++) {
    files.push('/' + randomstring.generate(10))
  }

  var ops = []
  var reference = {}

  for (i = 0; i < numLayers; i++) {
    var layerOps = []
    for (var j = 0; j < opsPerLayer; j++) {
      var name = randomItem(files)
      var contents = randomstring.generate(fileLength)
      layerOps.push({
        file: name,
        contents: contents
      })
      reference[name] = contents
    }
    ops.push(layerOps)
  }

  var layerCount = 0

  var baseLayer = Layerdrive(p.join(__dirname, 'data', base + '.tar'),
    driveFactory,
    { layerDir: TEST_DIR })
  baseLayer.ready(function (err) {
    if (err) return cb(err)
    return makeNextLayer(baseLayer)
  })

  function makeNextLayer (layer) {
    _applyOps(layer, ops[layerCount], commit)

    function commit (err) {
      if (err) return cb(err)
      if (layer.commit) {
        layer.commit(function (err, nextLayer) {
          if (err) return cb(err)
          finish(nextLayer)
        })
      } else {
        finish(layer)
      }
    }

    function finish (nextLayer) {
      layerCount++
      if (layerCount === numLayers) return cb(null, nextLayer, ops, reference)
      return makeNextLayer(nextLayer)
    }
  }
}

function assertValidReads (t, drive, files, cb) {
  var numFinished = 0
  var fileList = Object.keys(files)
  fileList.forEach(function (file) {
    drive.readFile(file, 'utf-8', function (err, contents) {
      if (!files[file]) {
        t.notEqual(err, null)
      } else {
        t.error(err)
        t.equal(contents, files[file])
      }
      if (++numFinished === fileList.length) return cb(null)
    })
  })
}
function assertValidReadstreams (t, drive, files, cb) {
  var numFinished = 0
  var fileList = Object.keys(files)
  fileList.forEach(function (file) {
    var readStream = drive.createReadStream(file)
    pump(readStream, concat(gotContents))
    function gotContents (contents) {
      t.equal(contents.toString('utf-8'), files[file])
      if (++numFinished === fileList.length) return cb(null)
    }
  })
}

test('read/write works for a single layer, single file', function (t) {
  createLayerdrive('alpine', 1, 1, 1, 100, function (err, drive, _, reference) {
    t.error(err)
    assertValidReads(t, drive, reference, function (err) {
      t.error(err)
      t.end()
    })
  })
})

test('read/write works for a single layer, single file, with streams', function (t) {
  createLayerdrive('alpine', 1, 1, 1, 100, function (err, drive, _, reference) {
    t.error(err)
    assertValidReadstreams(t, drive, reference, function (err) {
      t.error(err)
      t.end()
    })
  })
})

test('read/write works for a single layer, multiple files', function (t) {
  createLayerdrive('alpine', 1, 10, 5, 100, function (err, drive, _, reference) {
    t.error(err)
    assertValidReadstreams(t, drive, reference, function (err) {
      t.error(err)
      t.end()
    })
  })
})

test('read/write works for multiple layers, single file', function (t) {
  createLayerdrive('alpine', 2, 1, 1, 100, function (err, drive, _, reference) {
    t.error(err)
    assertValidReadstreams(t, drive, reference, function (err) {
      t.error(err)
      t.end()
    })
  })
})

test('read/write work for many layers, multiple files', function (t) {
  createLayerdrive('alpine', 7, 500, 100, 10, function (err, drive, _, reference) {
    t.error(err)
    assertValidReads(t, drive, reference, function (err) {
      t.error(err)
      t.end()
    })
  })
})

test('deletion', function (t) {
  createLayerdrive('alpine', 1, 1, 1, 100, function (err, drive, _, reference) {
    t.error(err)
    drive.writeFile('/hello', 'world', function (err) {
      t.error(err)
      drive.unlink('/hello', function (err) {
        t.error(err)
        drive.readFile('/hello', function (err, contents) {
          t.notEqual(err, undefined)
          t.end()
        })
      })
    })
  })
})

test('directory creation/reads/deletion', function (t) {
  // async/await's sounding sweet...
  createLayerdrive('alpine', 1, 1, 1, 100, function (err, drive, _, reference) {
    t.error(err)
    drive.mkdir('/hello_dir', function (err) {
      t.error(err)
      drive.writeFile('/hello_dir/world', 'goodbye', function (err) {
        t.error(err)
        drive.readdir('/hello_dir', function (err, files) {
          t.error(err)
          t.equal(files.length, 1)
          t.equal(files[0], '/hello_dir/world')
          drive.rmdir('/hello_dir', function (err) {
            t.notEqual(err, undefined)
            drive.unlink('/hello_dir/world', function (err) {
              t.error(err)
              drive.rmdir('/hello_dir', function (err) {
                t.error(err)
                t.end()
              })
            })
          })
        })
      })
    })
  })
})

test('stats', function (t) {
  createLayerdrive('alpine', 1, 1, 1, 100, function (err, drive, _, reference) {
    t.error(err)
    var file = Object.keys(reference)[0]
    drive.stat(file, function (err, firstStat) {
      t.error(err)
      var firstTime = firstStat.mtime
      t.equal(firstStat.size, 100)
      drive.writeFile(file, Buffer.alloc(10), function (err) {
        t.error(err)
        drive.stat(file, function (err, secondStat) {
          t.error(err)
          t.equal(secondStat.size, 10)
          t.true(secondStat.mtime > firstTime)
          drive.commit(function (err, newDrive) {
            t.error(err)
            newDrive.stat(file, function (err, finalStat) {
              t.error(err)
              t.equal(finalStat.size, 10)
              // Ensure that other metadata persists across writes.
              t.equal(finalStat.mode, firstStat.mode)
              t.end()
            })
          })
        })
      })
    })
  })
})

test('chown/chmod', function (t) {
  createLayerdrive('alpine', 1, 1, 1, 100, function (err, drive, _, reference) {
    t.error(err)
    var file = Object.keys(reference)[0]
    drive.chown(file, 10, 11, function (err, stat) {
      t.error(err)
      t.equal(stat.uid, 10)
      t.equal(stat.gid, 11)
      drive.chmod(file, 511, function (err, stat) {
        t.error(err)
        t.equal(stat.mode, 511)
        drive.commit(function (err, newDrive) {
          t.error(err)
          newDrive.stat(file, function (err, stat) {
            t.error(err)
            t.equal(stat.uid, 10)
            t.equal(stat.gid, 11)
            t.equal(stat.mode, 511)
            t.end()
          })
        })
      })
    })
  })
})

test('symlinking, equal stats', function (t) {
  createLayerdrive('alpine', 1, 1, 1, 100, function (err, drive, _, reference) {
    t.error(err)
    var file = Object.keys(reference)[0]
    drive.symlink(file, '/some_link', function (err) {
      t.error(err)
      drive.stat(file, function (err, fileStat) {
        t.error(err)
        drive.stat('/some_link', function (err, linkStat) {
          t.error(err)
          t.deepEqual(linkStat, fileStat)
          t.end()
        })
      })
    })
  })
})

test('cleanup', function (t) {
  rimraf.sync(TEST_DIR)
  t.end()
})

test('different layer storage')
test('replication')
