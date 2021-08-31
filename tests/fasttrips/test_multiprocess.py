import multiprocessing
import sys

import pytest

from fasttrips.Queue import QueueData, ProcessWorkerTask, ProcessManager, ExceptionQueueData


def queue_task(num):
    if num == 7:
        raise ValueError("7 not allowed")
    return num * 2


class DummyWorkerTask(ProcessWorkerTask):
    def __call__(
        self, *args, worker_num: int, in_queue: multiprocessing.Queue, out_queue: multiprocessing.Queue, **kwargs
    ):
        while True:
            res_flag = self.poll_queue(in_queue, out_queue, worker_num)
            if res_flag:
                return

    @staticmethod
    def poll_queue(in_queue: multiprocessing.Queue, out_queue: multiprocessing.Queue, worker_num: int) -> bool:
        state_obj: ExceptionQueueData = in_queue.get()

        if state_obj.state == QueueData.WORK_DONE:
            out_queue.put(QueueData(QueueData.WORK_DONE, worker_num))
            return True
        out_queue.put(ExceptionQueueData(QueueData.STARTING, worker_num, message=state_obj.message))
        try:
            out = queue_task(state_obj.message)
            out_queue.put(ExceptionQueueData(QueueData.COMPLETED, worker_num, message=out))
            return False
        except:
            out_queue.put(ExceptionQueueData(QueueData.EXCEPTION, worker_num, message=str(sys.exc_info())))
            return True


@pytest.fixture(params=[1, 2], ids=["1 process", "2 processes"])
def pm(request):
    num_processes = request.param
    return ProcessManager(
        num_processes=num_processes,
        process_worker_task=DummyWorkerTask(),
        process_worker_task_args=tuple(),
        wait_time=1,
    )


class TestProcessManager:
    def test_terminates(self, pm):
        for i in [1, 4, 9]:
            pm.todo_queue.put(ExceptionQueueData(QueueData.TO_PROCESS, message=i))

        # could do this with a context manager and be neater
        pm.add_work_done_sentinels()
        pm.start_processes()

        results = []

        def finalizer(queue_data: ExceptionQueueData):
            results.append(queue_data.message)

        pm.wait_and_finalize(finalizer)
        assert sum(results) == 28
        for process_id, process_dict in pm.process_dict.items():
            # check process marked as done
            assert process_dict["done"]
            # check process is actually stopped
            assert process_dict["alive"] is False
            assert process_dict["process"].is_alive() is False

    def test_exceptions_propagate(self, pm):
        for i in [1, 4, 7]:
            pm.todo_queue.put(ExceptionQueueData(QueueData.TO_PROCESS, message=i))

        # could do this with a context manager and be neater
        pm.add_work_done_sentinels()
        pm.start_processes()

        results = []

        def finalizer(queue_data: ExceptionQueueData):
            results.append(queue_data.message)

        with pytest.raises(ValueError, match="7 not allowed"):
            pm.wait_and_finalize(finalizer)

    def test_crashed_processes_noticed(self, pm):
        for i in [1, 4, 9]:
            pm.todo_queue.put(ExceptionQueueData(QueueData.TO_PROCESS, message=i))

        # could do this with a context manager and be neater
        pm.add_work_done_sentinels()
        pm.start_processes()
        # simulate a process dying unexpectedly
        list(pm.process_dict.values())[0]["process"].kill()
        # this happens instantly, before processes grab anything from queue
        # so as long as there is more than once process all data is processed.
        # A better test would be to delay this until the process had data, but this is hard to mock

        results = []

        def finalizer(queue_data: ExceptionQueueData):
            results.append(queue_data.message)

        pm.wait_and_finalize(finalizer)
        print(results)
        print(pm.process_dict)
        if pm.is_multiprocessed:  # code can continue aside from dead process
            # since nothing was on the killed process when it died, can continue as normal
            assert sum(results) == 28

            num_done = sum(1 for i in pm.process_dict.values() if i["done"])
            num_not_done = pm.num_processes - num_done
            assert num_not_done == 1
            assert num_done == pm.num_processes - 1

        else:  # only sub-process is dead
            assert len(results) == 0
            assert list(pm.process_dict.values())[0]["done"] is False
