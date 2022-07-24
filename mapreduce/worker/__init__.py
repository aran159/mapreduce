from concurrent import futures
import grpc
from driver_pb2 import (
    assignedTaskResult
)
from driver_pb2_grpc import DriverStub
from common_pb2 import empty
from mapreduce import constants
from worker_pb2_grpc import (
    WorkerServicer,
    add_WorkerServicer_to_server
)
import time
from mapreduce.model import (
    TaskType
)
from .map import map
from .reduce import reduce


server = None


class Worker(WorkerServicer):
    def __init__(self) -> None:
        super().__init__()
        self._channel = None
        self._stub = None

    def connect_to_driver(self) -> None:
        self._channel = grpc.insecure_channel(f'localhost:{constants.DRIVER_PORT}')
        self._stub = DriverStub(self._channel)

        # self._channel.close()

    def wait_for_driver(self):
        print("[Worker] Waiting for driver to be ready")
        response = self._stub.healthCheck(empty(), wait_for_ready=True)
        print("[Worker] Driver is ready")

    def main_loop(self):
        try:
            while True:
                response = self._stub.requestTaskAssignment(empty())
                assigned_task_id = response.id

                if assigned_task_id == constants.ALL_TASKS_COMPLETED.id:
                    print(f"[Worker] All tasks completed. Shutting down worker")
                    self.kill()

                if assigned_task_id == constants.TASK_NOT_ASSIGNED.id:
                    print(f"[Worker] No task assigned. Sleeping for {constants.WORKER_SLEEP_TIME_IN_SECONDS} seconds")
                    time.sleep(constants.WORKER_SLEEP_TIME_IN_SECONDS)
                    continue

                assigned_task_type = TaskType(response.taskType)
                print(f"[Worker] Executing {assigned_task_type.name} task {assigned_task_id}")
                try:
                    self.execute_task(assigned_task_type, assigned_task_id)
                    print(f"[Worker] Completed {assigned_task_type.name} task {assigned_task_id}")
                    self._stub.notifyTaskStatus(assignedTaskResult(taskType=assigned_task_type.value, id=assigned_task_id, statusCode=0))
                except Exception:
                    print(f"[Worker] Failed {assigned_task_type.name} task {assigned_task_id}")
                    self._stub.notifyTaskStatus(assignedTaskResult(taskType=assigned_task_type.value, id=assigned_task_id, statusCode=1))

                time.sleep(constants.WORKER_SLEEP_TIME_IN_SECONDS)
        except Exception as e:
            raise e

    def execute_task(self, task_type: TaskType, task_id: int):
        if task_type == TaskType.MAP:
            # TODO: M comes from the driver
            M = 1
            map(task_id, M)
        elif task_type == TaskType.REDUCE:
            reduce(task_id)
        else:
            raise ValueError

    def kill(self):
        global server
        server.stop(0)
        quit()

def run():
    port = constants.DRIVER_PORT + 1
    worker = Worker()

    global server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    add_WorkerServicer_to_server(worker, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()

    worker.connect_to_driver()
    worker.wait_for_driver()
    worker.main_loop()
