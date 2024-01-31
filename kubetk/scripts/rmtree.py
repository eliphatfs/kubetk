import os
import argparse
import textwrap
from queue import Queue
from threading import Lock
from multiprocessing.pool import ThreadPool


def main():
    iolock = Lock()
    dispatch_futures = Queue()

    def remove(p):
        os.unlink(p)
        if args.verbose:
            with iolock:
                print(p, flush=True)

    def dispatch(p):
        isempty = True
        for entry in os.scandir(p):
            isempty = False
            if entry.is_symlink() or entry.is_file(follow_symlinks=False):
                pool.apply_async(remove, [entry.path])
            elif entry.is_dir():
                dispatch_futures.put(pool.apply_async(dispatch, [entry.path]))
            else:
                raise TypeError("Unexpected FS entry type", entry.path)
        if isempty:
            os.removedirs(p)
            with iolock:
                print(p, flush=True)

    argp = argparse.ArgumentParser(description=textwrap.dedent("""
        Rapidly removes the directory and all its contents.
    """))
    argp.add_argument("targets", nargs='+')
    argp.add_argument("-v", "--verbose", action='store_true')
    argp.add_argument("--threads", type=int, default=32)
    args = argp.parse_args()
    pool = ThreadPool(args.threads)

    for sub in args.targets:
        dispatch_futures.put(pool.apply_async(dispatch, [sub]))
    while not dispatch_futures.empty():
        dispatch_futures.get().get()
    for sub in args.targets:
        dispatch_futures.put(pool.apply_async(dispatch, [sub, True]))
    while not dispatch_futures.empty():
        dispatch_futures.get().get()
    pool.close()
    pool.join()


if __name__ == '__main__':
    main()
