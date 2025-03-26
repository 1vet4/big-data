from detection.gps_spoofing import GpsSpoofingDetection
import pandas as pd

service = GpsSpoofingDetection()

if __name__ == "__main__":
    data_path = r'C:\Users\Iveta\PycharmProjects\parametric\aisdk-2025-03-11.csv'
    data = pd.read_csv(data_path, nrows=10000)

    one, two, three = service.process_data_in_parallel()
    #one, two, three = service.process_data_in_sequence()
