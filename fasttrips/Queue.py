"""Multiprocessing utilities around Queues.

Note this could potentially be simplified by using a multiprocess.Pool,
but the ability to debug specific cases and error messages is less straightforward.
"""
import abc
import datetime
import multiprocessing
import queue
import sys
import traceback
from abc import ABC
from typing import Callable, Any, Iterable, Tuple, Dict

from fasttrips import FastTripsLogger


class QueueData(object):
    """Collection of data to be passed on multiprocess.queue methods.

        Allows calls to put() and get() to have a consistent api:
            (msg_code, worker_num, QueueData)

        """

    # Defined data states
    DONE = "DONE"
    STARTING = "STARTING"
    COMPLETED="COMPLETED"

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

    def handle_entry(self):
        pass

    def wait_and_finalize(self, task_finalizer: Callable[[QueueData], None]):
        """Track state of processes and wait until they're done"""
        done_procs = 0  # where done means not alive
        while done_procs < len(self.process_dict):

            try:
                # poll results every 30 seconds
                result = self.done_queue.get(True, 30)
            except queue.Empty:
                pass
            else:
                worker_num = result.worker_num
                if result.state ==QueueData.DONE:
                    FastTripsLogger.debug("Received done from process %d" % worker_num)
                    self.process_dict[worker_num]["done"] = True
                elif result.state == QueueData.STARTING:
                    self.process_dict[worker_num]["working_on"] = result.identifier

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
                    FastTripsLogger.info("Process %d appears to have crashed; it was working on %s" % \
                                         (process_idx, str(indiv_process_dict["working_on"])))
                else:
                    FastTripsLogger.info("Process %d appears to have crashed; see ft_debug_worker%02d.log" % (
                        process_idx, process_idx))

    def exception_handler(self, exception):
        try:
            raise exception
        except (KeyboardInterrupt, SystemExit):
            exc_type, exc_value, exc_tb = sys.exc_info()
            FastTripsLogger.error("Exception caught: %s" % str(exc_type))
            error_lines = traceback.format_exception(exc_type, exc_value, exc_tb)
            for e in error_lines: FastTripsLogger.error(e)
            FastTripsLogger.error("Terminating processes")
            # terminating my processes
            for proc in self.process_dict.values():
                proc["process"].terminate()
            raise
        except:
            # some other error
            exc_type, exc_value, exc_tb = sys.exc_info()
            error_lines = traceback.format_exception(exc_type, exc_value, exc_tb)
            for e in error_lines: FastTripsLogger.error(e)
            raise
