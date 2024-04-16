import numpy as np
import grpc
from concurrent import futures
import kmeans_pb2
import kmeans_pb2_grpc

class Reducer(kmeans_pb2_grpc.ReducerServiceServicer):
    def __init__(self, reducer_id):
        self.reducer_id = reducer_id

    def ProcessReduce(self, request, context):
        # Unpack the received data
        data_points = {int(k): [list(map(float, point.split(','))) for point in v.points] for k, v in request.data_points.items()}
        
        # Calculate new centroids
        new_centroids = self.reduce(data_points)
        
        # Prepare response
        centroids = [kmeans_pb2.Centroid(coordinates=centroid) for centroid in new_centroids.values()]
        return kmeans_pb2.ProcessReduceResponse(success=True, message="Reduction completed successfully.", updated_centroids=centroids)

    def reduce(self, data_points):
        """Calculate the new centroids from the given data points."""
        new_centroids = {}
        for centroid_idx, points in data_points.items():
            if points:  # Ensure there are points to calculate the mean
                new_centroid = np.mean(points, axis=0).tolist()
                new_centroids[centroid_idx] = new_centroid
        return new_centroids

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    reducer = Reducer(reducer_id=1)  # Example setup
    kmeans_pb2_grpc.add_ReducerServiceServicer_to_server(reducer, server)
    server.add_insecure_port('[::]:50052')  # Adjust port as necessary
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
