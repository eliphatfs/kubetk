import os
import argparse
import textwrap
from threading import Lock
from kubetk.helpers.parallel_walk import parallel_walk


def main():
    iolock = Lock()

    def nil(_):
        pass

    def remove(p):
        os.unlink(p)
        if args.verbose:
            with iolock:
                print(p, flush=True)

    def rmdirs(p, entries):
        if not len(entries):
            os.removedirs(p)
            if args.verbose:
                with iolock:
                    print(p, flush=True)

    argp = argparse.ArgumentParser(description=textwrap.dedent("""
        Rapidly removes the directory and all its contents.
    """))
    argp.add_argument("targets", nargs='+')
    argp.add_argument("-v", "--verbose", action='store_true')
    argp.add_argument("--threads", type=int, default=32)
    args = argp.parse_args()

    parallel_walk(args.targets, remove, None, args.threads)
    parallel_walk(args.targets, nil, rmdirs, args.threads)


if __name__ == '__main__':
    main()
