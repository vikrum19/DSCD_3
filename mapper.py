import os
import numpy as np
import grpc
from concurrent import futures

# Import the generated Python code for gRPC
from kmeans_pb2 import ProcessChunkResponse
from kmeans_pb2_grpc import MapperServiceServicer, add_MapperServiceServicer_to_server

class Mapper(MapperServiceServicer):
    def __init__(self, mapper_id, num_reducers):
        self.mapper_id = mapper_id
        self.centroids = None
        self.num_reducers = num_reducers

    def ProcessChunk(self, request, context):
        self.centroids = [np.array(centroid.coordinates) for centroid in request.centroids]
        data_chunk = request.data_chunk  # Adjust this based on the actual data passed
        data_points = self.read_data(data_chunk)
        assignments = self.map(data_points)
        self.partition(assignments)
        return ProcessChunkResponse(success=True, message="Processed successfully.")

    def read_data(self, data_chunk):
        # Assuming data_chunk now directly contains data points
        return [list(map(float, point.split(','))) for point in data_chunk]

    def map(self, data_points):
        assignments = {}
        for point in data_points:
            distances = [np.linalg.norm(np.array(point) - centroid) for centroid in self.centroids]
            nearest_centroid_idx = distances.index(min(distances))
            if nearest_centroid_idx not in assignments:
                assignments[nearest_centroid_idx] = []
            assignments[nearest_centroid_idx].append(point)
        return assignments

    def partition(self, assignments):
        base_dir = f"./Mappers/M{self.mapper_id}"
        os.makedirs(base_dir, exist_ok=True)
        for centroid_idx, points in assignments.items():
            partition_idx = centroid_idx % self.num_reducers
            partition_file = os.path.join(base_dir, f"partition_{partition_idx}.txt")
            with open(partition_file, 'a') as file:
                for point in points:
                    file.write(f"{centroid_idx}: {','.join(map(str, point))}\n")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapper = Mapper(mapper_id=1, num_reducers=3)  # Example setup
    add_MapperServiceServicer_to_server(mapper, server)
    server.add_insecure_port('[::]:50051')  # Adjust port as necessary
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
