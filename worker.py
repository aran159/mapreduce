import grpc
from driver_pb2_grpc import DriverStub
from driver_pb2 import AssignTaskRequest


if __name__ == '__main__':
    channel = grpc.insecure_channel('localhost:50051')
    stub = DriverStub(channel)
    response = stub.assign_task(AssignTaskRequest(name='Worker'))
    channel.close()
    print("Client received: " + response.message)
