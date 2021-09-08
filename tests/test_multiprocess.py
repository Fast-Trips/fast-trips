import multiprocessing
import sys
import os

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


skip_mark = pytest.mark.skipif(
    os.environ.get("GITHUB_ACTIONS"),
    reason="MultiProcessed code seems to fail stochastically on GHA, but is fine locally",
)


@pytest.fixture(params=[1, pytest.param(3, marks=skip_mark)], ids=["Single process", "2 additional sub-processes"])
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

            message = queue_data.message
            results.append(message)

        pm.wait_and_finalize(finalizer)
        assert sum(results) == 28
        for process_id, process_dict in pm.process_dict.items():
            # check process marked as done
            assert process_dict.done
            # check process is actually stopped
            assert process_dict.alive is False
            assert process_dict.process.is_alive() is False

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
        if pm.is_multiprocessed is False:
            pytest.skip("Can't test multiprocess only crashes from serial code")

        for i in [1, 4, 9]:
            pm.todo_queue.put(ExceptionQueueData(QueueData.TO_PROCESS, message=i))

        pm.add_work_done_sentinels()

        for n, (process_idx, process_dict) in enumerate(pm.process_dict.items()):
            process_dict.process.start()
            if n == 0:
                if sys.version_info < (3, 7):
                    # ask nicely for python 3.6
                    process_dict.process.terminate()
                else:  # be forceful for python 3.7+
                    process_dict.process.kill()

        results = []

        def finalizer(queue_data: ExceptionQueueData):
            results.append(queue_data.message)

        pm.wait_and_finalize(finalizer)

        num_done = sum(1 for i in pm.process_dict.values() if i.done)
        num_not_done = pm.num_processes - num_done
        assert num_not_done == 1
        assert num_done == pm.num_processes - 1
        # note don't check the value of results, because this depends on whether
        # killed thread popped from the queue before it died
