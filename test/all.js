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
  drive.on('ready', onready)

  function onready () {
    var opIndex = 0
    if (ops.length !== 0) _nextOp()
    function _nextOp () {
      var op = ops[opIndex] 
      drive.writeFile(op.file, op.contents, function (err) {
        console.log('file written!')
        if (err) return cb(err)
        if (++opIndex === ops.length) return cb(null)
        _nextOp()
      })
    }
  }
}

function createBaseHyperdrive () {
  var drive = Hyperdrive(BASE_DIR)
  drives[drive.key] = drive
  return drive
}

function driveFactory(storage, key, opts) {
  var drive = Hyperdrive(storage, key, opts)
  if (drives[drive.key]) {
    var existingDrive = drives[drive.key] 
    var existingStream = existingDrive.replicate()
    existingStream.pipe(drive.replicate()).pipe(existingStream)
  } else {
    drives[drive.key] = drive
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
    ops.unshift(layerOps)
  }

  var curLayer = createBaseHyperdrive()
  var layerIndex = 0
  makeNextLayer()

  function  makeNextLayer() {
    _applyOps(curLayer, ops[layerIndex], prefinish)

    function prefinish (err) {
      if (err) return cb(err)
      if (curLayer.commit) {
        console.log('COMMITTING')
        return curLayer.commit(finish)
      }
      finish()
    }

    function finish() {
      var key = curLayer.key || curLayer.parent
      curLayer = Layerdrive(key, {
        layerStorage: TEST_DIR,
        driveFactory: driveFactory
      })
      if (layerIndex == numLayers - 1) return cb(null, curLayer, ops, reference)
      layerIndex++
      makeNextLayer()
    }
  }
}

function assertValidReads (t, drive, files, cb) {
  var numFinished = 0
  var fileList = Object.keys(files)
  fileList.forEach(function (file) {
    drive.readFile(file, 'utf-8', function (err, contents) {
      t.error(err)
      t.equal(contents, files[file])
      if (++numFinished === fileList.length) return cb(null)
    })
  })
}

function assertValidReadstreams (t, drive, files, cb) {
  var numFinished = 0
  var fileList = Object.keys(files)
  fileList.forEach(function (file) {
    var readStream = drive.createReadStream(file, { encoding: 'utf-8' })
    pump(readStream, concat(gotContents))
    function gotContents (contents) {
      t.equal(contents, files[file])
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
  createLayerdrive(5, 5, 15, 100, function (err, drive, _, reference) {
    t.error(err)
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
