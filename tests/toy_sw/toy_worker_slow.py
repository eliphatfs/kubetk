import time
import random
import kubetk.arch.worker as wk


def compute(x):
    return x + 1


def slow(x):
    time.sleep(1)
    if random.random() < 0.1:
        raise ValueError('bad luck')
    return x * x


def main(pipeline=False):
    server = "http://127.0.0.1:9105"
    if pipeline:
        wk.run_pipelined_worker(server, [(compute, 1), (slow, 2)])
    else:
        wk.run_simple_worker(server, lambda x: slow(compute(x)))


if __name__ == '__main__':
    main(True)
