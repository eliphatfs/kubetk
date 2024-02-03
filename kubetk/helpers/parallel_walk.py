import os
from typing import Callable, Any, List
from queue import Queue
from multiprocessing.pool import ThreadPool


def parallel_walk(
    roots,
    callback_entry: Callable[[str], Any],
    callback_directory: Callable[[str, List[os.DirEntry]], Any] = None,
    num_threads: int = 32
):
    dispatch_futures = Queue()
    pool = ThreadPool(num_threads)

    def dispatch(p):
        entries = []
        for entry in os.scandir(p):
            entries.append(entry)
            if entry.is_symlink() or entry.is_file(follow_symlinks=False):
                pool.apply_async(callback_entry, [entry.path])
            elif entry.is_dir():
                dispatch_futures.put(pool.apply_async(dispatch, [entry.path]))
            else:
                raise TypeError("Unexpected FS entry type", entry.path)
        if callback_directory is not None:
            dispatch_futures.put(pool.apply_async(callback_directory, [p, entries]))

    def bootstrap(p):
        if os.path.isdir(p):
            dispatch_futures.put(pool.apply_async(dispatch, [p]))
        else:
            dispatch_futures.put(pool.apply_async(callback_entry, [p]))
    
    pool.map(bootstrap, roots)
    while not dispatch_futures.empty():
        dispatch_futures.get().get()
    pool.close()
    pool.join()
