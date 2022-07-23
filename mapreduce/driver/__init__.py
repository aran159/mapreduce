from pathlib import Path
import mapreduce.constants as constants
import grpc
from driver_pb2_grpc import (
    DriverServicer,
    add_DriverServicer_to_server
)
from concurrent.futures import ThreadPoolExecutor
from mapreduce import utils
from common_pb2 import empty


class Driver(DriverServicer):
    def __init__(self, input_dir: str, N: int, M: int) -> None:
        super().__init__()

        self._input_dir = input_dir
        self._N = N
        self._M = M

        self.initialize_tmp_folder()

    def requestTaskAssignment(self, request, context):
        print(f'Task assigned')
        return empty()

    def start(self) -> None:
        pass
        #
        # Split files into N
        # Create a map state variable and start sending tasks to workers when asked for
        # When all map tasks are completed, start sending reduce tasks to workers
        # When all reduce tasks return, send a signal to terminate workers and terminate self

    @staticmethod
    def initialize_tmp_folder() -> None:
        utils.remove_tmp_dir()
        Path(constants.TMP_DIR).mkdir(exist_ok=True)


def serve(N: int, M: int, input_dir: str):
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    add_DriverServicer_to_server(Driver(input_dir, N, M), server)
    server.add_insecure_port(f'[::]:{constants.DRIVER_PORT}')
    server.start()
    server.wait_for_termination()
