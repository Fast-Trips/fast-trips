"""Multiprocessing utilities around Queues.

Note this could potentially be simplified by using a multiprocess.Pool,
but the ability to debug specific cases and error messages is less straightforward.

These utilities are structured to be compatible with the multiprocessing implementation in Assignment.py,
but have been abstracted away to support reuse in other contexts (namely skimming).

To reduce code duplication, Assignment.py should be updated to make use of these utilities directly.
"""
import abc
import multiprocessing
import queue
import sys
import traceback
from abc import ABC
from typing import Callable, Any, Tuple

from .Logger import FastTripsLogger


class QueueData(object):
    """Collection of data to be passed on multiprocess.queue methods.

    Allows calls to put() and get() to have a consistent api:
        (msg_code, worker_num, QueueData)

    """

    # Defined data states
    WORK_DONE = "WORK_DONE"  # all tasks completed (input queue empty)
    TO_PROCESS = "TO_PROCESS"  # specified input task to process (input queue)
    STARTING = "STARTING"  # specific task started (state change on output queue)
    COMPLETED = "COMPLETED"  # specific task completed (output queue)
    EXCEPTION = "EXCEPTION"  # specific task crashed (output queue)

    def __init__(self, state: str, worker_num=None, *args, **kwargs):
        """

        Args:
            state: [QueueData.DONE, QueueData.STARTING, QueueData.COMPLETED,
                    (other string code which is not handled directly)]
            worker_num: process number
        """
        self.state = state
        self.worker_num = worker_num
        self.identifier = None  # string containing info to help debug thread crash


class ExceptionQueueData(QueueData):
    def __init__(self, state: str, worker_num=None, message="", *args, **kwargs):
        super().__init__(state, worker_num, *args, **kwargs)
        self.message = message


class ProcessWorkerTask(ABC):
    """
    Interface for process worker tasks. Not a fully fledged class, using
    to provide a consistent signature to callables.

    See test_multiprocess.py for the minimal requirements
    """

    @abc.abstractmethod
    def __call__(
        self, *args, worker_num: int, in_queue: multiprocessing.Queue, out_queue: multiprocessing.Queue, **kwargs
    ):
        """Function/ code to be run on each process with supplied arguments.
        Note worker num and queues are supplied as keyword only argument as this will be populated by
        ProcessManager.

        Should contain initial setup, followed by while True loop to handle in_queue events

        Currently does not support kwargs when used with ProcessManager

        Args:
            *args:
            worker_num:
            in_queue (Queue[QueueData]): events to be processed by worker
            out_queue (Queue[QueueData]): results to be collected from worker
            **kwargs:
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def poll_queue(in_queue: multiprocessing.Queue, out_queue: multiprocessing.Queue, worker_num: int) -> bool:
        """Process first entry at front of supplied queue.
        Returns True if threads work is done (either finished or exception)
        False if not.

        Must handle 4 states: QueueData.STARTING, QueueData.COMPLETED, QueueData.EXCEPTION, QueueData.WORK_DONE

        """
        pass


class ProcessManager(object):
    def __init__(
        self,
        num_processes: int,
        process_worker_task: ProcessWorkerTask,
        process_worker_task_args: Tuple[Any, ...],
        wait_time: int = 15,
    ):
        """

        Args:
            num_processes (int): number of processes to launch (if negative or zero, use cpu count)
            process_worker_task (ProcessWorkerTask): subclass of ProcessWorkerTask
            process_worker_task_args (Tuple[Any, ...]): arguments to process_worker_task,
                except worker_num which is passed internally as a kwargs
            wait_time (int): seconds main process should wait in-between polling the status of child processes.
        """
        if num_processes < 1:
            num_processes = multiprocessing.cpu_count()

        self.num_processes = num_processes
        self.is_multiprocessed = num_processes > 1
        self.wait_time = wait_time

        self.process_dict = {}
        # data to pass to workers
        self.todo_queue = multiprocessing.Queue()
        # results to retrieve from workers
        self.done_queue = multiprocessing.Queue()

        # stable kwargs
        kwargs = {"in_queue": self.todo_queue, "out_queue": self.done_queue}

        for process_idx in range(1, 1 + num_processes):
            FastTripsLogger.info("Creating worker process %2d" % process_idx)
            # kwarg per process
            kwargs["worker_num"] = process_idx

            self.process_dict[process_idx] = {
                "process": multiprocessing.Process(
                    target=process_worker_task, args=process_worker_task_args, kwargs=kwargs
                ),
                "alive": True,
                "done": False,
            }

    def start_processes(self):

        for process_idx, process_dict in self.process_dict.items():
            FastTripsLogger.info("Starting worker process %2d" % process_idx)
            print("started process ", process_idx)
            process_dict["process"].start()

    def add_work_done_sentinels(self):
        """Add sentinel entries to the end of the input queue, so there is a
        definitive, thread safe way to know when the work is finished."""
        for _ in range(len(self.process_dict)):
            self.todo_queue.put(QueueData(QueueData.WORK_DONE))

    def wait_and_finalize(self, task_finalizer: Callable[[QueueData], None]):
        """Track state of processes and wait until they're done.

        Args:
            task_finalizer (Callable[[QueueData], None): callback triggered to process results received from processes.
                Likely a function to pass results back into main program scope and out of queue, by
                dumping results into another data structure.


        """
        done_procs = 0  # where done means not alive
        while done_procs < len(self.process_dict):

            try:
                # poll results every 30 seconds
                if self.is_multiprocessed:
                    wait_time = self.wait_time
                else:
                    # if we're testing a simple serial case, we don't want waiting to be most of the runtime
                    wait_time = min(self.wait_time, 2)
                result = self.done_queue.get(True, wait_time)
            except queue.Empty:
                pass
            else:
                worker_num = result.worker_num
                if result.state == QueueData.WORK_DONE:
                    FastTripsLogger.debug("Received done from process %d" % worker_num)
                    self.process_dict[worker_num]["done"] = True
                elif result.state == QueueData.STARTING:
                    self.process_dict[worker_num]["working_on"] = result.identifier
                elif result.state == QueueData.EXCEPTION:
                    raise ValueError(f"Worker num {worker_num} produced an exception:\n {result.message}")

                elif result.state == QueueData.COMPLETED:
                    task_finalizer(result)
                    del self.process_dict[worker_num]["working_on"]

            # check if any processes are not alive
            for process_idx, indiv_process_dict in self.process_dict.items():
                if indiv_process_dict["alive"] and not indiv_process_dict["process"].is_alive():
                    FastTripsLogger.debug("Process %d is not alive" % process_idx)
                    indiv_process_dict["alive"] = False
                    done_procs += 1

        # join up my processes
        for indiv_process_dict in self.process_dict.values():
            indiv_process_dict["process"].join()

        # check if any processes crashed
        for process_idx, indiv_process_dict in self.process_dict.items():
            if not indiv_process_dict["done"]:
                if "working_on" in indiv_process_dict:
                    FastTripsLogger.info(
                        "Process %d appears to have crashed; it was working on %s"
                        % (process_idx, str(indiv_process_dict["working_on"]))
                    )
                else:
                    FastTripsLogger.info(
                        "Process %d appears to have crashed; see ft_debug_worker%02d.log" % (process_idx, process_idx)
                    )

    def exception_handler(self, exception):
        try:
            raise exception
        except (KeyboardInterrupt, SystemExit):
            exc_type, exc_value, exc_tb = sys.exc_info()
            FastTripsLogger.error("Exception caught: %s" % str(exc_type))
            error_lines = traceback.format_exception(exc_type, exc_value, exc_tb)
            for e in error_lines:
                FastTripsLogger.error(e)
            FastTripsLogger.error("Terminating processes")
            # terminating my processes
            for proc in self.process_dict.values():
                proc["process"].terminate()
            raise
        except:
            # some other error
            exc_type, exc_value, exc_tb = sys.exc_info()
            error_lines = traceback.format_exception(exc_type, exc_value, exc_tb)
            for e in error_lines:
                FastTripsLogger.error(e)
            raise
