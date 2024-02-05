from typing import *
from threading import Thread
from queue import Queue, Full
from multiprocessing import TimeoutError
from multiprocessing.pool import ThreadPool


drain_sentinel = object()


class NullQueue(Queue):
    def put(self, item: Any, block=True, timeout=None) -> None:
        return None


def pipeline_run(
    input_stream: Iterable[Any],
    spec: Sequence[Tuple[Callable[[Any], Any], int]],
    input_buffer: int = 1
):
    pools = [ThreadPool(n) for _, n in spec]
    try:
        queues = [Queue(input_buffer)] + [Queue(n) for _, n in spec[:-1]] + [NullQueue()]

        def stage(fn, rq: Queue, wq: Queue):
            while True:
                x = rq.get()
                if x is drain_sentinel:
                    break
                wq.put(fn(x))

        drain_signal = False

        def drain(q: Queue):
            while not drain_signal:
                q.put(drain_sentinel)

        sub_exception = None

        def on_sub_error(exc):
            nonlocal sub_exception
            sub_exception = exc

        def loop_check_error(single, timeout_type):
            while sub_exception is None:
                try:
                    return single()
                except timeout_type:
                    pass
            if sub_exception is not None:
                raise sub_exception

        futures = [
            p.starmap_async(stage, [(f, r, w)] * n, chunksize=1, error_callback=on_sub_error)
            for p, r, w, (f, n) in zip(pools, queues[:-1], queues[1:], spec)
        ]
        for x in input_stream:
            loop_check_error(lambda: queues[0].put(x, timeout=0.1), Full)
        for q, task in zip(queues, futures):
            drain_signal = False
            thread = Thread(target=drain, args=[q])
            thread.start()
            loop_check_error(lambda: task.get(0.1), TimeoutError)
            drain_signal = True
            while q.full():
                q.get()
            thread.join()
    finally:
        for p in pools:
            p.terminate()
