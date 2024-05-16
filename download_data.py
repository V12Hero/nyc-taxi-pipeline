import os
import requests

def download_tlc_data(year, month, taxi_type):
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    filename = f"parquet_files/{taxi_type}_tripdata_{year}-{month:02d}.parquet"

    if os.path.exists(filename):
        print(f"{filename} already exists. Skipping download.")
        return True

    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded {filename}")
        return True
    else:
        print(f"Failed to download data for {taxi_type} {year}-{month:02d}. Status code: {response.status_code}")
        return False
