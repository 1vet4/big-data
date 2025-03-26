import pandas as pd
import numpy as np
import time
from geopy.distance import geodesic
from concurrent.futures import ProcessPoolExecutor


class GpsSpoofingDetection:

    def __init__(self):
        self.lat_threshold = 0.1  # degrees
        self.lon_threshold = 0.1  # degrees

        self.sog_threshold_high = 200 
        self.sog_threshold_low = 0.5
        self.distance_threshold_high = 10 # kilometers

        self.max_distance_between_vessels = 0.5  # kilometers
        self.chunk_size = 20000
        self.overlap_size = 1000
        self.num_workers = 8

        self.data_path = r'C:\Users\Iveta\PycharmProjects\parametric\aisdk-2025-03-11.csv'

    def haversine_distance(self, lat1, lon1, lat2, lon2):
        return geodesic((lat1, lon1), (lat2, lon2)).kilometers

    def detect_speed_anomalies(self, chunk):
        speed_anomalies = chunk[(chunk["SOG"] > self.sog_threshold_high)]
        return speed_anomalies

    def detect_distance_anomalies(self, chunk):
        chunk["distance"] = chunk.apply(
            lambda row: self.haversine_distance(row["Latitude"], row["Longitude"], row["prev_lat"], row["prev_lon"])
            if pd.notnull(row["prev_lat"]) and pd.notnull(row["prev_lon"]) else np.nan, axis=1
        )
        chunk["time_diff"] = (chunk["# Timestamp"] - chunk[
            "prev_timestamp"]).dt.total_seconds() / 3600  # Convert to hours
        chunk["speed"] = np.where(chunk["time_diff"] > 0.0003, chunk["distance"] / chunk["time_diff"], np.nan)

        distance_anomalies = chunk[(chunk["speed"] > self.sog_threshold_high)]
        return chunk, distance_anomalies

    def detect_position_anomalies(self, chunk):
        chunk["lat_diff_prev"] = (chunk["Latitude"] - chunk["prev_lat"]).abs()
        chunk["lat_diff_next"] = (chunk["Latitude"] - chunk["next_lat"]).abs()
        chunk["lon_diff_prev"] = (chunk["Longitude"] - chunk["prev_lon"]).abs()
        chunk["lon_diff_next"] = (chunk["Longitude"] - chunk["next_lon"]).abs()

        anomalies = chunk[(chunk["lat_diff_prev"] > self.lat_threshold) |
                          (chunk["lat_diff_next"] > self.lat_threshold) |
                          (chunk["lon_diff_prev"] > self.lon_threshold) |
                          (chunk["lon_diff_next"] > self.lon_threshold)]
        return chunk, anomalies

    def clean_data(self, chunk):
        chunk = chunk.dropna(subset=["Latitude", "Longitude", "SOG", "COG"])
        chunk = chunk[(chunk['Latitude'] >= -90) & (chunk['Latitude'] <= 90)]
        chunk = chunk[(chunk['Longitude'] >= -180) & (chunk['Longitude'] <= 180)]

        return chunk

    def process_chunk(self, chunk):
        chunk["# Timestamp"] = pd.to_datetime(chunk["# Timestamp"], format="%d/%m/%Y %H:%M:%S")

        chunk = chunk.sort_values(by=["MMSI", "# Timestamp"], ascending=[True, True])

        chunk = self.clean_data(chunk)

        # Detect location anomalies
        chunk["prev_lat"] = chunk.groupby("MMSI")["Latitude"].shift(1)
        chunk["next_lat"] = chunk.groupby("MMSI")["Latitude"].shift(-1)
        chunk["prev_lon"] = chunk.groupby("MMSI")["Longitude"].shift(1)
        chunk["next_lon"] = chunk.groupby("MMSI")["Longitude"].shift(-1)

        chunk['prev_timestamp'] = chunk.groupby("MMSI")["# Timestamp"].shift(1)
        chunk['next_timestamp'] = chunk.groupby("MMSI")["# Timestamp"].shift(-1)

        chunk, distance_anomalies = self.detect_distance_anomalies(chunk)
        chunk, position_anomalies = self.detect_position_anomalies(chunk)
        speed_anomalies = self.detect_speed_anomalies(chunk)

        return distance_anomalies.to_dict("records"), position_anomalies.to_dict("records"), speed_anomalies.to_dict(
            "records")

    def process_data_in_sequence(self):
        all_distance_anomalies = []
        all_position_anomalies = []
        all_speed_anomalies = []

        number_of_chunks = 0
        prev_chunk = None

        start_time = time.time()

        for chunk in pd.read_csv(self.data_path, chunksize=self.chunk_size):
            number_of_chunks += 1
            #print(f'Chunk number {number_of_chunks}')
            if prev_chunk is not None:
                # Concatenate with previous chunk overlap
                chunk = pd.concat([prev_chunk, chunk]).reset_index(drop=True)

            # Process chunk
            distance_anomalies, position_anomalies, speed_anomalies = self.process_chunk(chunk)
            all_distance_anomalies.append(distance_anomalies)
            all_position_anomalies.append(position_anomalies)
            all_speed_anomalies.append(speed_anomalies)

            prev_chunk = chunk.iloc[-self.overlap_size:]

        end_time = time.time()
        print(f"Sequential processing time: {end_time - start_time:.2f} seconds")
        return all_distance_anomalies, all_position_anomalies, all_speed_anomalies

    def process_data_in_parallel(self):
        all_distance_anomalies = []
        all_position_anomalies = []
        all_speed_anomalies = []
        start_time = time.time()
        num_chunks = 0
        prev_chunk = None

        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            futures = []
            for chunk in pd.read_csv(self.data_path, chunksize=self.chunk_size):
                num_chunks += 1
                if prev_chunk is not None:
                    # Concatenate with previous chunk overlap
                    chunk = pd.concat([prev_chunk, chunk]).reset_index(drop=True)

                futures.append(executor.submit(self.process_chunk, chunk))
                prev_chunk = chunk.iloc[-self.overlap_size:]

            for idx, future in enumerate(futures, 1):
                try:
                    result = future.result()
                    if result is not None:
                        dist_anomalies, pos_anomalies, spd_anomalies = result
                        if dist_anomalies is not None:
                            all_distance_anomalies.append(dist_anomalies)
                        if pos_anomalies is not None:
                            all_position_anomalies.append(pos_anomalies)
                        if spd_anomalies is not None:
                            all_speed_anomalies.append(spd_anomalies)
                except Exception as e:
                    print(f"Error in chunk {idx}: {e}")

        end_time = time.time()
        total_time = end_time - start_time
        print(f"Processed {num_chunks} chunks in {total_time:.2f} seconds.")

        return all_distance_anomalies, all_position_anomalies, all_speed_anomalies
