from detection.gps_spoofing import GpsSpoofingDetection
import pandas as pd

service = GpsSpoofingDetection()

if __name__ == "__main__":
    data_path = r'C:\Users\Iveta\PycharmProjects\parametric\aisdk-2025-03-11.csv'

    distance_anomalies, position_anomalies, speed_anomalies = service.process_data_in_parallel()
    #distance_anomalies, position_anomalies, speed_anomalies = service.process_data_in_sequence()
