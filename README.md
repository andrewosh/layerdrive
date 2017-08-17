## layerdrive
Layerdrive is a union filesystem where each layer can be independently distributed as a [Hyperdrive](https://www.github.com/mafintosh/hyperdrive). 

### Install
(WIP: this isn't live yet)
```
npm i layerdrive
```

### Usage
The Layerdrive API is, like Hyperdrive, designed to be consistent with Node FS.

```js
TODO: this should be an example.
```

### Internals
A layerdrive consists of three components:
1. A single metadata archive.
2. N layer archives.
3. A single copy-on-write layer, which is currently a temporary directory.

The metadata archive stores a filesystem index that enables fast lookup of file modifications, to determine which layer to read from for a given file. All writes are buffered in a copy-on-write layer, which is reified into an additional layer archive when the layerdrive is committed.

By default, all layer archives are synced in sparse mode.

#### Copy-on-write layer
Before a layerdrive is committed, all pending changes are written to a temporary directory. Any files already in the layerdrive (having been modified in one of the layer archives) will be copied to the temporary directory whenever they're first modified.

On commit, a new layer archive is created out of the temporary directory, and a metadata archive that references this new layer is created. Currently this is a finalizing operation -- no additional operations can be performed on the layerdrive after the commit. Once committed, the layerdrive's key is set to the new metadata archive's key.

#### Metadata archive
The metadata archive is lightweight -- it contains two files of interest:
1. `layerdrive.json`: A JSON file of metadata, with the most important record being the ordered list of N layer hyperdrive keys. By referencing each layer by its list index, the filesystem index can be kept small.
2. `layerdrive.db.tar`: A tarball'd LevelDB database containing a filesystem index. Each KV-pair is a mapping from filename to the last layer that modified it.

Before any operations are performed on a layerdrive, the entire metadata archive must be synced (but none of the layer archives need to be synced!), and the filesystem index must be untar'd.

#### Layer archives
Each layer archive is...well...just the hyperdrive of that copy-on-write layer's changes. It's only useful when referenced by a metadata archive. Importantly, if a layer only modifies a few files, then the resulting layer archive will be very small. Not much more to this component.

#### Reads
A read operation consults the filesystem index for the appropriate layer to read from, then reads from it. Only that layer needs to be synced for the read operation to complete.

#### Writes
Writes are slightly trickier: If a file exists in one of the read layers, it must first be copied to the writable layer (the temporary directory) in its entirety.

#### Stat updates (symlinking, chmod, chown...)
As of now, these operations performed directly on cached stat objects, and are written to the most recent layer archive's append-tree on commit. Ultimately, these changes might be merged into Hyperdrive upstream.

Note: The writable layer is currently a temporary directory in order to support random-access writes.

Deletions are straightforward: The filesystem index is updated to set the last-modifier of the deleted file to the latest layer, and that file is unlinked.

#### Creating Hyperdrives
Since a layerdrive needs to create a hyperdrive for each of its layers, it must be provided with a `driveFactory` function that will instantiate the drive with suitable storage, and handle replication. These two features are not handled directly by layerdrive.

### API
TODO: API description

## License
MIT
