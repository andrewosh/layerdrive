var concat = require('concat-stream')
var test = require('tape')
var pump = require('pump')

var testUtil = require('.')
var createLayerdrive = testUtil.createLayerdrive

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
    assertValidReadstreams(t, drive, reference, function (err) {
      t.error(err)
      t.end()
    })
  })
})

test('moving', function (t) {
  createLayerdrive('alpine', 7, 500, 100, 10, function (err, drive, _, reference) {
    t.error(err)
    drive.writeFile('/hello', 'world', function (err) {
      t.error(err)
      drive.chmod('/hello', '655', function (err) {
        t.error(err)
        drive.mv('/hello', '/goodbye', function (err) {
          t.error(err)
          drive.readFile('/goodbye', 'utf-8', function (err, contents) {
            t.error(err)
            t.equal(contents, 'world')
            drive.stat('/goodbye', function (err, stat) {
              t.error(err)
              stat.mode = '655'
              drive.stat('/hello', function (err) {
                t.notEqual(err, undefined)
                t.end()
              })
            })
          })
        })
      })
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

test('appending, existing file', function (t) {
  createLayerdrive('alpine', 1, 1, 1, 100, function (err, drive, _, reference) {
    t.error(err)
    drive.writeFile('/hello', 'world', function (err) {
      t.error(err)
      drive.append('/hello', 'blah', function (err) {
        t.error(err)
        drive.readFile('/hello', 'utf-8', function (err, contents) {
          t.error(err)
          t.equal(contents, 'worldblah')
          t.end()
        })
      })
    })
  })
})

test('appending, new file', function (t) {
  createLayerdrive('alpine', 1, 1, 1, 100, function (err, drive, _, reference) {
    t.error(err)
    drive.append('/hello', '', function (err) {
      t.error(err)
      drive.stat('/hello', function (err, stat) {
        t.error(err)
        t.equal(stat.size, 0)
        t.end()
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

test('symlinking, original file deletion', function (t) {
  createLayerdrive('alpine', 1, 1, 1, 100, function (err, drive, _, reference) {
    t.error(err)
    var file = Object.keys(reference)[0]
    drive.symlink(file, '/some_link', function (err) {
      t.error(err)
      drive.unlink(file, function (err) {
        t.error(err)
        drive.stat('/some_link', function (err, entry) {
          t.notEqual(err, undefined)
          drive.stat('/some_link', { noFollow: true }, function (err, entry) {
            t.error(err)
            t.equal(entry.linkname, file)
            t.end()
          })
        })
      })
    })
  })
})

test('cleanup', function (t) {
  testUtil.cleanUp()
  t.end()
})

test('different layer storage')
test('replication')
