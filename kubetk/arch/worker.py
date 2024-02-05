from typing import *
from xmlrpc.client import ServerProxy

from .pipeline import pipeline_run


def run_simple_worker(uri: str, handler: Callable[[Any], Any]):
    with get_rpc_object(uri) as rpc:
        for work in iterate_sched(rpc):
            try:
                handler(work)
            except Exception as exc:
                rpc.error(work, repr(exc))
            else:
                rpc.done(work)


def iterate_sched(uri: str):
    with get_rpc_object(uri) as rpc:
        while True:
            work = rpc.get()
            if work is None:
                break
            yield work


def get_rpc_object(uri: str):
    return ServerProxy(uri, allow_none=True)


def run_pipelined_worker(uri: str, pipeline_specs: Sequence[Tuple[Callable[[Any], Any], int]]):

    def _input_iter():
        for work in iterate_sched(uri):
            yield (work, work, None)

    def _wrap(fn):

        def _fn(x):
            work, arg, error = x
            if error is not None:
                return work, arg, error
            try:
                return work, fn(arg), None
            except Exception as exc:
                return work, fn, exc

        return _fn

    def _report(x):
        work, arg, error = x
        with get_rpc_object(uri) as rpc:
            if error is None:
                rpc.done(work)
            else:
                rpc.error(work, repr(error) + " in " + repr(arg))

    pipeline_run(_input_iter(), [(_wrap(f), n) for f, n in pipeline_specs] + [(_report, 2)])
