import os
import kubetk.arch.worker as wk


def compute(x):
    return x + 1


def write_simple(x):
    os.makedirs("test_tmp", exist_ok=True)
    with open("test_tmp/%d.txt" % x, "w") as fo:
        print("simple", file=fo)


def write_pipelined(x):
    os.makedirs("test_tmp", exist_ok=True)
    with open("test_tmp/%d.txt" % x, "w") as fo:
        print("pipelined", file=fo)


def main(pipeline=False):
    server = "http://127.0.0.1:9105"
    if pipeline:
        wk.run_pipelined_worker(server, [(compute, 1), (write_pipelined, 2)])
    else:
        wk.run_simple_worker(server, lambda x: write_simple(compute(x)))


if __name__ == '__main__':
    main(True)
