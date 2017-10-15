var p = require('path')

var cuid = require('cuid')
var mkdirp = require('mkdirp')
var randomstring = require('randomstring')
var randomItem = require('random-item')
var rimraf = require('rimraf')

var debug = require('debug')('layerdrive')

var datEncoding = require('dat-encoding')
var Hyperdrive = require('hyperdrive')
var Layerdrive = require('..')

var TEST_DIR = './test-layers'

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

function driveFactory (key, opts, cb) {
  mkdirp.sync(TEST_DIR)

  var id = key ? datEncoding.toStr(key) : cuid()
  var storage = p.join(TEST_DIR, id)
  var drive = Hyperdrive(storage, key, opts)
  drive.on('ready', function () {
    var existingDrive = drives[drive.key]
    if (existingDrive) {
      var existingStream = existingDrive.replicate()
      existingStream.pipe(drive.replicate()).pipe(existingStream)
    } else {
      drives[drive.key] = drive
    }
    return cb(null, drive)
  })
}

function createLayerdrive (base, numLayers, numFiles, opsPerLayer, fileLength, cb) {
  debug('createLayerdrive', base, numLayers, numFiles, opsPerLayer, fileLength)
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
        layer.commit(function (err) {
          if (err) return cb(err)
          finish(layer)
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

function cleanUp () {
  rimraf.sync(TEST_DIR)
}

module.exports = {
  createLayerdrive: createLayerdrive,
  driveFactory: driveFactory,
  cleanUp: cleanUp
}
