import grpc
import xenon_pb2
import xenon_pb2_grpc


def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = xenon_pb2_grpc.XenonJobsStub(channel)
    response = stub.getSchemes(xenon_pb2.Empty())
    print(response)

if __name__ == '__main__':
    run()