import grpc
from driver_pb2_grpc import DriverStub
from common_pb2 import empty
from mapreduce import constants


def run():
    channel = grpc.insecure_channel(f'localhost:{constants.DRIVER_PORT}')
    stub = DriverStub(channel)
    response = stub.requestTaskAssignment(empty())
    print("Driver responded")
    channel.close()
