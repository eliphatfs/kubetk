import os
import stat
import zipfile
import argparse
import textwrap
from threading import Lock
from kubetk.helpers.parallel_walk import parallel_walk


def main():
    ziplock = Lock()

    def archive(p):
        with open(p, "rb") as fi:
            contents = fi.read()
            with ziplock:
                zipf.writestr(os.path.relpath(p).replace(os.path.pathsep, '/'), contents)

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
    with zipfile.ZipFile(args.output, "w") as zipf:
        parallel_walk(args.inputs, archive, num_threads=args.read_threads)
        if args.verbose:
            for name in zipf.namelist():
                print(name)
        if not args.quiet:
            print(len(zipf.namelist()), "files archived.")


if __name__ == '__main__':
    main()
