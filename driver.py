from pathlib import Path
from shutil import rmtree
import constants
import grpc
from driver_pb2 import (
    AssignTaskRequest,
    AsignTaskReply,
)
from driver_pb2_grpc import (
    DriverServicer,
    add_DriverServicer_to_server
)
from concurrent.futures import ThreadPoolExecutor
import utils


class Driver(DriverServicer):
    def __init__(self, input_dir: str, N: int, M: int) -> None:
        super().__init__()

        self._input_dir = input_dir
        self._N = N
        self._M = M

        self.initialize_tmp_folder()

    def assign_task(self, request, context):
        print(f'Task {request.name} assigned')
        return AsignTaskReply(message='Hello')

    def start(self) -> None:
        pass
        # Split files into N
        # Create a map state variable and start sending tasks to workers when asked for
        # When all map tasks are completed, start sending reduce tasks to workers
        # When all reduce tasks return, send a signal to terminate workers and terminate self

    @staticmethod
    def initialize_tmp_folder() -> None:
        utils.remove_tmp_dir()
        Path(constants.TMP_DIR).mkdir(exist_ok=True)


def serve():
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    add_DriverServicer_to_server(Driver('inputs/', 10000, 100), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()