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
        isfile = os.path.isfile(p) or os.path.islink(p)
        isdir = not isfile and os.path.isdir(p)
        assert isfile or isdir
        if isfile:
            pool.apply_async(remove, [p])
        if isdir:
            ls = os.listdir(p)
            if not ls:
                os.rmdir(p)
                if args.verbose:
                    with iolock:
                        print(p, flush=True)
            for s in ls:
                dispatch_futures.put(pool.apply_async(dispatch, [os.path.join(p, s)]))

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
        dispatch_futures.put(pool.apply_async(dispatch, [sub]))
    while not dispatch_futures.empty():
        dispatch_futures.get().get()
    pool.close()
    pool.join()


if __name__ == '__main__':
    main()
