"""Multiprocessing utilities around Queues.

Note this could potentially be simplified by using a multiprocess.Pool,
but the ability to debug specific cases and error messages is less straightforward.

These utilities are structured to be compatible with the multiprocessing implementation in Assignment.py,
but have been abstracted away to support reuse in other contexts (namely skimming).

To reduce code duplication, Assignment.py should be updated to make use of these utilities directly.
If a true single process implementation is required, ProcessManager should be subclassed as
SingleProcessManager so that a consistent api of method calls can be used in both cases.
"""
import abc
import multiprocessing
import queue
import sys
import traceback
from abc import ABC
from typing import Callable, Any, Tuple, Dict, Union, Optional

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


class MockProcess(object):
    """Mock object around a callable to mimic being a separate multiprocessing.Process process"""

    def __init__(self, target, args, kwargs):
        """

        Args:
            target (ProcessWorkerTask): subclass of ProcessWorkerTask
            args (Tuple[Any, ...]): arguments to process_worker_task,
            process_worker_task_kwargs (Dict[str, Any]): keyword arguments to process_worker_task
        """
        self.task = lambda: target(args, **kwargs)
        self.result = None

    def start(self):
        self.result = self.task()
        return self.result

    # mocked attributes

    def join(self):
        pass  # wait for the process to finish, in serial case this is as soon as start() is run

    def is_alive(self):
        # process is always dead (technically instantly alive during self.task(), but it's serial so
        # we can't query it during that point)
        return False


class ProcessWrapper(object):
    """
    Wrapper object around Process (or MockProcess) with some state flags attached. More explicit to work
    with an reason about than a dictionary with multiple keys for state.
    """

    def __init__(
        self,
        process: Union[multiprocessing.Process, MockProcess],
        alive=True,
        done=False,
        working_on: Optional[str] = None,
    ):
        """
        Args:
            process Process object / interface

        """
        self.process = process
        self.alive = alive
        self.done = done
        self.working_on = working_on


class ProcessManager(object):
    """User facing interface for (potentially) spawning multiple processes to speed up a discrete task.

    Internally delegates handles the special case where no additional processes are spawned by
    mocking the interface of multiprocess.Process.
    """

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

        self.process_dict: Dict[int, ProcessWrapper] = {}

        if self.is_multiprocessed:
            process_constructor = multiprocessing.Process
        else:
            process_constructor = MockProcess
        print(f"STDOUT Running in multiprocessed mode {self.is_multiprocessed}")
        FastTripsLogger.info(f"Running in multiprocessed mode {self.is_multiprocessed}")

        if num_processes == 2:
            FastTripsLogger.warning(
                "num_processes is the total number of processes including the parent process. "
                "Using num_processes=2 will offer no benefit over num_processes=1, as only "
                "one sub-process is created."
            )

        # data to pass to workers (semi reduntant for single process, essentially a list)
        self.todo_queue = multiprocessing.Queue()
        # results to retrieve from workers
        self.done_queue = multiprocessing.Queue()

        # stable kwargs
        kwargs = {"in_queue": self.todo_queue, "out_queue": self.done_queue}

        for process_idx in range(1, 1 + num_processes):  # plus 1 to have 1 based indexing and consistency
            FastTripsLogger.info("Creating worker sub-process %2d" % process_idx)
            # kwarg per process
            kwargs["worker_num"] = process_idx

            process = process_constructor(target=process_worker_task, args=process_worker_task_args, kwargs=kwargs)
            self.process_dict[process_idx] = ProcessWrapper(process)

    def start_processes(self):

        for process_idx, process_wrapper in self.process_dict.items():
            FastTripsLogger.info(
                f"Starting worker process {process_idx} " + "" if self.is_multiprocessed else "(mock process)"
            )
            process_wrapper.process.start()

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
                    # if we're testing a simple serial case, we don't need to wait
                    wait_time = 0.0

                result = self.done_queue.get(True, wait_time)

            except queue.Empty:
                # serial, empty queue means we are done, multiprocess, dead process means we are done
                if self.is_multiprocessed is False:
                    # single entry in dict
                    for process_idx, process_wrapper in self.process_dict.items():
                        process_wrapper.alive = False
                    break

            else:
                worker_num = result.worker_num
                if result.state == QueueData.WORK_DONE:
                    FastTripsLogger.debug("Received done from process %d" % worker_num)
                    self.process_dict[worker_num].done = True
                elif result.state == QueueData.STARTING:
                    self.process_dict[worker_num].working_on = result.identifier
                elif result.state == QueueData.EXCEPTION:
                    raise ValueError(f"Worker num {worker_num} produced an exception:\n {result.message}")

                elif result.state == QueueData.COMPLETED:
                    task_finalizer(result)
                    self.process_dict[worker_num].working_on = None

            # check if any processes are not alive
            if self.is_multiprocessed:
                for process_idx, process_wrapper in self.process_dict.items():
                    if process_wrapper.alive and not process_wrapper.process.is_alive():
                        FastTripsLogger.debug("Process %d is not alive" % process_idx)
                        process_wrapper.alive = False
                        # serial, empty queue means we are done, multiprocess, dead process means we are done
                        done_procs += 1

        # join up my processes
        for process_wrapper in self.process_dict.values():
            process_wrapper.process.join()

        # check if any processes crashed
        for process_idx, process_wrapper in self.process_dict.items():
            if not process_wrapper.done:
                if process_wrapper.working_on is not None:
                    FastTripsLogger.info(
                        "Process %d appears to have crashed; it was working on %s"
                        % (process_idx, str(process_wrapper.working_on))
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
                proc.process.terminate()
            raise
        except:
            # some other error
            exc_type, exc_value, exc_tb = sys.exc_info()
            error_lines = traceback.format_exception(exc_type, exc_value, exc_tb)
            for e in error_lines:
                FastTripsLogger.error(e)
            raise
