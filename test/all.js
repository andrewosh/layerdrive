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
  console.log('here')
  drive.on('error', function (err) {
    return cb(err)
  })
  drive.ready(onready)

  function onready () {
    console.log('and here')
    var opIndex = 0
    if (ops.length !== 0) _nextOp()
    function _nextOp () {
      var op = ops[opIndex] 
      console.log('writing', op.contents, 'to', op.file)
      drive.writeFile(op.file, op.contents, function (err) {
        console.log('file written!')
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
    console.log('base key:', drive.key.toString('hex'))
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
    console.log('drive key in replicate:', key.toString('hex'))
    var existingDrive = drives[key]
    if (existingDrive) {
      console.log('before replicate')
      var newStream = drive.replicate()
      console.log('after replicate')
      console.log('replicating drives w/key:', key.toString('hex'))
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
      console.log('in finish')
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
        console.log('the damn', file, 'should be there as', files[file])
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
    readStream.on('data', function (data) {
      console.log('data:', data)
    })
    pump(readStream, concat(gotContents))
    function gotContents (contents) {
      t.equal(contents.toString('utf-8'), files[file])
      if (++numFinished === fileList.length) return cb(null)
    }
  })
}

test('read layer without writing', function (t) {
  createLayerdrive(1, 1, 1, 100, function (err, drive, _, reference) {
    t.error(err)
    assertValidReads(t, drive, reference, function (err) {
      t.error(err)
      t.end()
    })
  })
})

test('read layer without writing, from stream', function (t) {
  createLayerdrive(1, 1, 1, 100, function (err, drive, _, reference) {
    t.error(err)
    assertValidReadstreams(t, drive, reference, function (err) {
      t.error(err)
      t.end()
    })
  })
})

test('multiple read layers without writing', function (t) {
  createLayerdrive(20, 1000, 10, 10, function (err, drive, _, reference) {
    t.error(err)
    console.log('lastModifiers:', drive.lastModifiers)
    assertValidReads(t, drive, reference, function (err) {
      t.error(err)
      t.end()
    })
  })
})

/*
test('read/write with one layer, different layers')
test('read/write with one layer, reuse files')
test('read/write with multiple layers')
test('layer storage')
test('replication')
*/
