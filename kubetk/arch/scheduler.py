import time
import threading
from queue import Queue, Empty
from collections import defaultdict, deque
from kubetk.helpers import rpc_server


class WorkQueue(object):
    def __init__(
        self,
        init_workload: list = None,
        backoff: int = 1,
        error_message_limit: int = 100
    ) -> None:
        self.queue = Queue()
        self.ongoing = set()
        self.error_counts = defaultdict(lambda: 0)
        self.total_success = 0
        self.total_backoff_success = 0
        self.total_backoff_failure = 0
        self.backoff = backoff
        self.error_lock = threading.Lock()
        self.messages = deque()
        self.error_message_limit = error_message_limit
        if init_workload is not None:
            for item in init_workload:
                self.put(item)

    def get(self):
        try:
            workload = self.queue.get_nowait()
            self.ongoing.add(workload)
            return workload
        except Empty:
            return None

    def put(self, workload):
        if workload is None:
            raise ValueError("WorkQueue workload cannot be `None`")
        self.queue.put_nowait(workload)

    def error(self, workload, message: str):
        self.ongoing.remove(workload)
        with self.error_lock:
            ec = self.error_counts[workload]
            self.error_counts[workload] += 1
            if len(self.messages) >= self.error_message_limit:
                self.messages.popleft()
            self.messages.append(message)
        if ec < self.backoff:
            self.put(workload)
        else:
            self.total_backoff_failure += 1

    def done(self, workload):
        self.ongoing.remove(workload)
        if workload in self.error_counts:
            self.total_backoff_success += 1
        self.total_success += 1


class Statistics(object):

    def __init__(self, work_queue: WorkQueue, period: float = 30.0) -> None:
        self.work_queue = work_queue
        self.period = period
        self.speed = 0
        threading.Thread(target=self._update_stats, daemon=True).start()

    def _update_stats(self):
        while True:
            start = self.work_queue.queue.qsize()
            time.sleep(self.period)
            self.speed = self.work_queue.queue.qsize() - start

    def stat(self):
        return dict(
            remain=self.work_queue.queue.qsize(),
            ongoing=len(self.work_queue.ongoing),
            success=self.work_queue.total_success,
            backoff_success=self.work_queue.total_backoff_success,
            backoff_failure=self.work_queue.total_backoff_failure,
            throughput=max(0.0, self.speed / self.period),
            eta=0.0 if self.speed <= 0 else self.work_queue.queue.qsize() / self.speed * self.period
        )

    def list_errors(self):
        with self.work_queue.error_lock:
            return {str(k): v for k, v in self.work_queue.error_counts.items()}

    def list_messages(self):
        with self.work_queue.error_lock:
            return list(self.work_queue.messages)


def serve_scheduler(work_queue: WorkQueue, stats_period: float = 30.0, port: int = 9105):
    stats = Statistics(work_queue, stats_period)
    with rpc_server.threaded(port) as server:
        server.register_introspection_functions()
        server.register_multicall_functions()
        server.register_function(work_queue.get)
        server.register_function(work_queue.put)
        server.register_function(work_queue.done)
        server.register_function(work_queue.error)
        server.register_function(stats.stat)
        server.register_function(stats.list_errors)
        server.register_function(stats.list_messages)
        server.serve_forever(0.5)
