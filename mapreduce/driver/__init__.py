from math import ceil
from pathlib import Path
from typing import List
import mapreduce.constants as constants
import grpc
from driver_pb2_grpc import (
    DriverServicer,
    add_DriverServicer_to_server
)
from concurrent.futures import ThreadPoolExecutor
from mapreduce import utils
from driver_pb2 import (
    assignedTask,
    healthCheckResult
)
from common_pb2 import empty
from worker_pb2_grpc import WorkerStub
from .split import (
    split_files,
    dir_line_count,
    file_lengths,
)
from glob import glob
from mapreduce.model import (
    TaskStatus,
    TaskType,
    Tasks,
    Task
)


class Driver(DriverServicer):
    def __init__(self, input_dir: str, N: int, M: int) -> None:
        super().__init__()

        self._input_dir = input_dir
        self._N = N
        self._M = M

        self.tasks = None
        self.worker_ports: List[int] = []

    def healthCheck(self, request, context):
        print('[Driver] Health check')
        return healthCheckResult(M=self._M)

    def initialize_task_list(self) -> None:
        self.tasks = Tasks([
            *[Task(id, TaskType.MAP) for id in range(self._N)],
            *[Task(id, TaskType.REDUCE) for id in range(self._M)],
        ])
        print('[Driver] Task list initialized')

    def requestTaskAssignment(self, request, context):
        print(f'[Driver] Task requested')
        if self.tasks.tasks_terminated:
            print(f'[Driver] All tasks done. Shutting down driver and workers')
            return constants.ALL_TASKS_COMPLETED

        pending_tasks = self.tasks.filter(status=TaskStatus.PENDING)

        if not len(pending_tasks) > 0:
            print(f'[Driver] No more tasks pending')
            return constants.TASK_NOT_ASSIGNED

        pending_map_tasks = pending_tasks.filter(type=TaskType.MAP)
        if len(pending_map_tasks) > 0:
            assigned_task = pending_map_tasks[0]
            self.tasks.set_status(assigned_task.type, assigned_task.id, TaskStatus.IN_PROGRESS)
            print(f'[Driver] Map task assigned')
            return assignedTask(taskType=assigned_task.type.value, id=assigned_task.id)

        if not self.tasks.map_tasks_terminated:
            print(f'[Driver] Map tasks still in course. Wait until they are terminated')
            return constants.TASK_NOT_ASSIGNED
        else:
            pending_reduce_tasks = pending_tasks.filter(type=TaskType.REDUCE)
            assigned_task = pending_reduce_tasks[0]
            self.tasks.set_status(assigned_task.type, assigned_task.id, TaskStatus.IN_PROGRESS)
            print(f'[Driver] Reduce task assigned')
            return assignedTask(taskType=assigned_task.type.value, id=assigned_task.id)

    def notifyTaskStatus(self, request, context) -> None:
        notified_task_id = request.id
        notified_task_type = TaskType(request.taskType)
        status_code = request.statusCode

        if status_code == 0:
            print(f'[Driver] {notified_task_type.name} task {notified_task_id} was completed by worker')
            self.tasks.set_status(notified_task_type, notified_task_id, TaskStatus.DONE)
            return empty()
        else:
            print(f'[Driver] {notified_task_type.name} task {notified_task_id} failed at worker')
            self.tasks.set_status(notified_task_type, notified_task_id, TaskStatus.PENDING)
            return empty()

    def terminate(self) -> None:
        channel = grpc.insecure_channel(f'localhost:{constants.DRIVER_PORT + 1}')
        stub = WorkerStub(channel)
        stub.kill(empty())
        self.kill()

    def kill(self) -> None:
        global server
        server.stop(0)
        quit()


    @staticmethod
    def split_input_files(N: int, input_dir: str) -> None:
        input_file_paths = glob(str(Path(input_dir) / '*'))
        total_line_count = dir_line_count(input_file_paths)
        split_files(input_file_paths, file_lengths(total_line_count, N))
        print('[Driver] Input files splitted. Ready to start with map tasks')


def serve(N: int, M: int, input_dir: str):
    driver = Driver(input_dir, N, M)

    global server
    server = grpc.server(ThreadPoolExecutor(max_workers=1))
    add_DriverServicer_to_server(driver, server)
    server.add_insecure_port(f'[::]:{constants.DRIVER_PORT}')
    server.start()

    utils.remove_tmp_dir()
    driver.split_input_files(N, input_dir)
    driver.initialize_task_list()

    server.wait_for_termination()
