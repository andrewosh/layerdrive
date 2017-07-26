var p = require('path')

var concat = require('concat-stream')
var test = require('tape')
var pump = require('pump')
var mkdirp = require('mkdirp')
var rimraf = require('rimraf')
var ram = require('random-access-memory')
var randomstring = require('randomstring')
var randomItem = require('random-item')

var Layerdrive = require('..')
var Hyperdrive = require('hyperdrive')

var TEST_DIR = './test-layers'
var BASE_DIR = p.join(TEST_DIR, 'base')
mkdirp.sync(TEST_DIR)

var drives = {}

function _applyOps(drive, ops, cb) {
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

function createBaseHyperdrive (cb) {
  var drive = Hyperdrive(BASE_DIR)
  drive.ready(function (err) {
    if (err) return cb(err)
    drives[drive.key] = drive
    return cb(null, drive)
  })
}

function driveFactory(storage, key, opts) {
  var drive = Hyperdrive(storage, key, opts)
  drive.ready(function (err) {
    if (err) return drive.emit('error', err)
    replicate(drive.key)
  })
  function replicate (key) {
    var existingDrive = drives[key]
    if (existingDrive) {
      var newStream = drive.replicate()
      newStream.pipe(existingDrive.replicate()).pipe(newStream)
    } else {
      drives[key] = drive
    }
  }
  return drive
}
      
function createLayerdrive (numLayers, numFiles, opsPerLayer, fileLength, cb) {
  var files = []
  for (var i = 0; i < numFiles; i++) {
    files.push('/' + randomstring.generate(10))
  }

  var ops = []
  var reference = {}

  for (var i = 0; i < numLayers; i++) {
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
  createBaseHyperdrive(makeNextLayer)

  function makeNextLayer (err, layer) {
    if (err) return cb(err)

    _applyOps(layer, ops[layerCount], commit)

    function commit (err) {
      if (err) return cb(err)
      if (layer.commit) {
        layer.commit(function (err) {
          if (err) return cb(err)
          finish(layer.key)
        })
      } else {
        finish(layer.key)
      }
    }

    function finish (key) {
      layerCount++
      if (layerCount == numLayers) return cb(null, layer, ops, reference)
      makeNextLayer(null, Layerdrive(key, {
        layerStorage: p.join(TEST_DIR, key.toString('hex')),
        driveFactory: driveFactory
      }))
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
  createLayerdrive(2, 1, 1, 100, function (err, drive, _, reference) {
    t.error(err)
    assertValidReads(t, drive, reference, function (err) {
      t.error(err)
      t.end()
    })
  })
})

test('read/write works for a single layer, single file, with streams', function (t) {
  createLayerdrive(2, 1, 1, 100, function (err, drive, _, reference) {
    t.error(err)
    assertValidReadstreams(t, drive, reference, function (err) {
      t.error(err)
      t.end()
    })
  })
})

test('read/write works for a single layer, multiple files', function (t) {
  createLayerdrive(2, 10, 10, 100, function (err, drive, _, reference) {
    t.error(err)
    assertValidReadstreams(t, drive, reference, function (err) {
      t.error(err)
      t.end()
    })
  })
})

test('read/write work for many layers, multiple files', function (t) {
  createLayerdrive(20, 100, 100, 100, function (err, drive, _, reference) {
    t.error(err)
    assertValidReadstreams(t, drive, reference, function (err) {
      t.error(err)
      t.end()
    })
  })
})

test('custom layer storage works')
test('replication works for multiple layerdrives', function (t) {
  
})
