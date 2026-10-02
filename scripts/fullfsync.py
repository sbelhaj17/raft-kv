"""Time F_FULLFSYNC on macOS, alone and from several processes at once.

    python3 scripts/fullfsync.py            # 1 process, then 3 at once, 3 s each
    python3 scripts/fullfsync.py 5 2        # 5 processes, 2 s

Java's FileChannel.force is F_FULLFSYNC on macOS: it waits until the drive
has written its cache to flash, not just until the OS has handed it the data.
Each process appends 100 bytes and flushes, in a loop, to its own file in a
temporary directory. This is the cost every raft-kv node pays per sync, and
the second run shows what happens when three nodes share one drive.
"""

import fcntl
import os
import statistics
import sys
import tempfile
import time
from multiprocessing import Process, Queue


def worker(path, seconds, out):
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_APPEND)
    times = []
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        os.write(fd, b"x" * 100)
        t0 = time.perf_counter()
        fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
        times.append(time.perf_counter() - t0)
    os.close(fd)
    out.put(times)


def trial(procs, seconds, folder):
    out = Queue()
    ps = [Process(target=worker, args=(os.path.join(folder, f"f{i}"), seconds, out)) for i in range(procs)]
    for p in ps:
        p.start()
    results = [out.get() for _ in ps]
    for p in ps:
        p.join()
    every = [t for r in results for t in r]
    print(f"{procs} process(es): {len(every) / seconds:.0f} flushes/s in total, "
          f"median {statistics.median(every) * 1e3:.2f} ms each")


if __name__ == "__main__":
    many = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    seconds = float(sys.argv[2]) if len(sys.argv) > 2 else 3.0
    with tempfile.TemporaryDirectory() as folder:
        trial(1, seconds, folder)
        trial(many, seconds, folder)
