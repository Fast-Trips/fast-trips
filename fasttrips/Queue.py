"""Multiprocessing utilities around Queues.

Note this could potentially be simplified by using a multiprocess.Pool,
but the ability to debug specific cases and error messages is less straightforward.
"""
import abc
import datetime
import multiprocessing
from abc import ABC
from typing import Callable, Any, Iterable, Tuple, Dict

from fasttrips import FastTripsLogger


class QueueData(object):
    """Collection of data to be passed on multiprocess.queue methods.

    Allows calls to put() and get() to have a consistent api:
        (msg_code, worker_num, QueueData)

    """

    def __init__(self, state: str, *args, **kwargs):
        self.state = state


class ExceptionQueueData(QueueData):
    def __init__(self, state: str, message, *args, **kwargs):
        super().__init__(state)
        self.message = message


class ProcessWorkerTask(ABC):
    """
    Interface for process worker tasks. Not a fully fledged class, using
    to provide a consistent signature to callables.
    """

    @abc.abstractmethod
    def __call__(self, in_queue: multiprocessing.Queue, out_queue: multiprocessing.Queue, *args, worker_num: int, **kwargs):
        """Function/ code to be run on each process with supplied arguments.
        Note worker num is supplied as keyword only argument as this will be populated by
        process manager.

        Should contain initial setup, followed by while True loop to handle in_queue events

        Currently does not support kwargs when used with ProcessManager

        Args:
            in_queue: events to be processed by worker (worker_num, msg_code, QueueData)
            out_queue: results to be collected from worker (worker_num, msg_code, QueueData)
            *args:
            worker_num:
            **kwargs:
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def poll_queue(in_queue: multiprocessing.Queue, out_queue: multiprocessing.Queue, worker_num: int) -> bool:
        """Process first entry at front of supplied queue.
        Returns True if threads work is done (either finished or exception)
        False if not.
        """
        pass



class ProcessManager(object):
    def __init__(self, num_processes: int, process_worker_task: ProcessWorkerTask,
                 process_worker_task_args: Tuple[Any]):
        """

        Args:
            num_processes (int): number of processes to launch (if negative or zero, use cpu count)
            process_worker_task (ProcessWorkerTask): subclass of ProcessWorkerTask
            process_worker_task_args (Tuple[Any]): arguments to process_worker_task,
                except worker_num which is passed internally as a kwargs
        """
        if num_processes < 1:
            num_processes = multiprocessing.cpu_count()

        self.process_dict = {}
        # data to pass to workers
        self.todo_queue = multiprocessing.Queue()
        # results to retrieve from workers
        self.done_queue = multiprocessing.Queue()
        for process_idx in range(1, 1 + num_processes):
            FastTripsLogger.info("Starting worker process %2d" % process_idx)
            self.process_dict[process_idx] = {
                "process": multiprocessing.Process(target=process_worker_task,
                                                   args=process_worker_task_args,
                                                   kwargs={"worker_num":process_idx}),
                "alive": True,
                "done": False
            }


    def start_processes(self):
        for process_dict in self.process_dict.values():
            process_dict["process"].start()
        pass

    def handle_entry(self):
        pass

    def finalize(self):
        pass
