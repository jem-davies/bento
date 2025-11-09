from concurrent import futures
import logging 
import json
import math
import time

import grpc 
import bento_python_pb2
import bento_python_pb2_grpc
from grpc_reflection.v1alpha import reflection

def process(input_message):
### <script>
### </script>
    return str(output_message)

class BentoPythonServicer(bento_python_pb2_grpc.BentoPythonServicer):
    def Process(self, request, context):
        x = json.loads(request.data)
        msg = process(x)
        return bento_python_pb2.PythonRsp(data=msg.encode("utf-8"))

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    bento_python_pb2_grpc.add_BentoPythonServicer_to_server(
        BentoPythonServicer(), server
    )
    SERVICE_NAMES = (
        bento_python_pb2.DESCRIPTOR.services_by_name["BentoPython"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig()
    serve()