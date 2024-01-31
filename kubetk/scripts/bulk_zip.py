import os
import zipfile
import argparse
import textwrap
from queue import Queue
from threading import Lock
from multiprocessing.pool import ThreadPool


def main():
    ziplock = Lock()
    dispatch_futures = Queue()

    def archive(p):
        with open(p, "rb") as fi:
            contents = fi.read()
            with ziplock:
                zipf.writestr(os.path.relpath(p).replace(os.path.pathsep, '/'), contents)

    def dispatch(p):
        isfile = os.path.isfile(p)
        isdir = os.path.isdir(p)
        assert isfile or isdir
        if isfile:
            pool.apply_async(archive, [p])
        if isdir:
            for s in os.listdir(p):
                dispatch_futures.put(pool.apply_async(dispatch, [os.path.join(p, s)]))

    argp = argparse.ArgumentParser(description=textwrap.dedent("""
        Archive (store) the inputs into output zip file.
        Recurses into directories.
        Reads in parallel to memory then writes to zip.
    """))
    argp.add_argument("output")
    argp.add_argument("inputs", nargs='+')
    argp.add_argument("-q", "--quiet", action='store_true')
    argp.add_argument("-v", "--verbose", action='store_true')
    argp.add_argument("--read-threads", type=int, default=32)
    args = argp.parse_args()
    pool = ThreadPool(args.read_threads)
    with zipfile.ZipFile(args.output, "w") as zipf:
        for sub in args.inputs:
            dispatch_futures.put(pool.apply_async(dispatch, [sub]))
        while not dispatch_futures.empty():
            dispatch_futures.get().get()
        pool.close()
        pool.join()
        if args.verbose:
            for name in zipf.namelist():
                print(name)
        if not args.quiet:
            print(len(zipf.namelist()), "files archived.")


if __name__ == '__main__':
    main()
