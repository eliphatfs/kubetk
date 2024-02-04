import os
import shutil
import argparse
import textwrap
from threading import Lock
from kubetk.helpers.parallel_walk import parallel_walk


def main():
    iolock = Lock()

    def copy(p):
        dst = os.path.join(args.dst, os.path.relpath(p, args.src))
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        if args.verbose:
            with iolock:
                print(p, '->', dst, flush=True)
        shutil.copyfile(p, dst, follow_symlinks=False)

    def cdir(p, _):
        dst = os.path.join(args.dst, os.path.relpath(p, args.src))
        os.makedirs(dst, exist_ok=True)
        if args.verbose:
            with iolock:
                print(p, '->', dst, flush=True)

    argp = argparse.ArgumentParser(description=textwrap.dedent("""
        `cp -r src dst`, but concurrently writing multiple files.
    """))
    argp.add_argument("src")
    argp.add_argument("dst")
    argp.add_argument("-v", "--verbose", action='store_true')
    argp.add_argument("--threads", type=int, default=32)
    args = argp.parse_args()
    args.src = os.path.normpath(args.src)
    args.dst = os.path.normpath(args.dst)
    if os.path.isdir(args.dst):
        args.dst = os.path.join(args.dst, os.path.basename(args.src))
    parallel_walk([args.src], copy, cdir, num_threads=args.threads)


if __name__ == '__main__':
    main()
