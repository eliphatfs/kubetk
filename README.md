# kubetk
`kubetk` is a set of tools designed to utilize a cluster with high-latency volumes better.

## Installation

```bash
pip install git+https://github.com/eliphatfs/kubetk.git
```

## Single-Node CLI Scripts

These scripts use concurrency to boost the throughput of high-latency volumes.

### `kubetk-bulk-zip`

The script reads files concurrently, then stores into a zip file without compression.
The script stores contents of files of the threads are reading in memory.
It is suitable for archiving large amount of small files before transfer.
It also recurses into directories with good concurrency.

#### Benchmark

Cross-zone (files stored in *us-west* and execution node is in *us-central*) archiving 5000 small thumbnails that sum up to 559MB:

| Binary | Threads | Wall Time (s) |
| :----: | :-----: | :--: |
| `zip` | - | 1216 |
| `kubetk-bulk-zip` | `32` (default) | 32.0 |
| `kubetk-bulk-zip` | `256` | 13.9 |

### `kubetk-rmtree`

The script recurses into directories and removes all the contents concurrently.

#### Benchmark

Cross-zone (files stored in *us-east* and execution node is in *us-central*):

| Binary | Threads | Throughput (files/s) |
| :----: | :-----: | :--: |
| `rm` | - | 14.01 |
| `kubetk-rmtree` | `32` (default) | 15.52 |
| `kubetk-rmtree` | `1024` | 15.93 |
