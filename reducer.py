import grpc
import numpy as np
from mapreduce_pb2 import GetRequest
from mapreduce_pb2_grpc import MapReduceServiceStub

# Assuming a proto file that defines gRPC services:
# service MapReduceService {
#   rpc GetKeyValuePairs(GetRequest) returns (KeyValuePairStream);
# }
# where KeyValuePairStream returns a stream of key-value pairs (centroid_id, point) for the given reducer.
# Also assuming that the `GetRequest` can specify which mapper's results the reducer should retrieve.

class Reducer:
    def __init__(self, reducer_id, mappers_addresses, output_directory):
        self.reducer_id = reducer_id
        self.mappers_addresses = mappers_addresses
        self.output_directory = output_directory
        self.channels = [grpc.insecure_channel(address) for address in mappers_addresses]
        self.stubs = [MapReduceServiceStub(channel) for channel in self.channels]
    
    def reduce(self):
        centroid_accumulator = {}

        # Collecting all key-value pairs for this reducer from all mappers
        for stub in self.stubs:
            try:
                for key_value_pair in stub.GetKeyValuePairs(GetRequest(reducer_id=self.reducer_id)):
                    key, point = key_value_pair.key, np.array(key_value_pair.value)
                    if key not in centroid_accumulator:
                        centroid_accumulator[key] = []
                    centroid_accumulator[key].append(point)
            except grpc.RpcError as e:
                print(f"An error occurred: {e}")
                continue  

        # Sort and group by centroid ID (key) if not ordered
        sorted_centroid_keys = sorted(centroid_accumulator.keys())
        sorted_accumulator = {key: centroid_accumulator[key] for key in sorted_centroid_keys}

        # Calculating new centroids and writing them to a file
        output_file = f"{self.output_directory}/R{self.reducer_id}.txt"
        with open(output_file, 'w') as file:
            for centroid_id, points in sorted_accumulator.items():
                new_centroid = np.mean(points, axis=0)
                file.write(f"{centroid_id}:{new_centroid.tolist()}\n")

        print(f"Reducer {self.reducer_id} has completed processing and written to {output_file}.")

# Example 
# reducer = Reducer(reducer_id=1, mappers_addresses=["localhost:50051", "localhost:50052"], output_directory="Data/Reducers/")
# reducer.reduce()
