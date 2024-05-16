import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fastavro
from datetime import datetime
from download_data import download_tlc_data

os.makedirs('parquet_files', exist_ok=True)
os.makedirs('processed_parquet_files', exist_ok=True)
os.makedirs('processed_avro_files', exist_ok=True)

parquet_schema = pa.schema([
    ('VendorID', pa.int64()),
    ('pickup_datetime', pa.timestamp('s')),
    ('dropoff_datetime', pa.timestamp('s')),
    ('store_and_fwd_flag', pa.string()),
    ('RatecodeID', pa.int64()),
    ('PULocationID', pa.int64()),
    ('DOLocationID', pa.int64()),
    ('passenger_count', pa.int64()),
    ('trip_distance', pa.float64()),
    ('fare_amount', pa.float64()),
    ('extra', pa.float64()),
    ('mta_tax', pa.float64()),
    ('tip_amount', pa.float64()),
    ('tolls_amount', pa.float64()),
    ('improvement_surcharge', pa.float64()),
    ('total_amount', pa.float64()),
    ('payment_type', pa.int64()),
    ('congestion_surcharge', pa.float64()),
    ('taxi_type', pa.string())
])

avro_schema = {
    "type": "record",
    "name": "TaxiTrip",
    "fields": [
        {"name": "VendorID", "type": ["null", "long"], "default": None},
        {"name": "pickup_datetime", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
        {"name": "dropoff_datetime", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
        {"name": "store_and_fwd_flag", "type": ["null", "string"], "default": None},
        {"name": "RatecodeID", "type": ["null", "long"], "default": None},
        {"name": "PULocationID", "type": ["null", "long"], "default": None},
        {"name": "DOLocationID", "type": ["null", "long"], "default": None},
        {"name": "passenger_count", "type": ["null", "long"], "default": None},
        {"name": "trip_distance", "type": ["null", "double"], "default": None},
        {"name": "fare_amount", "type": ["null", "double"], "default": None},
        {"name": "extra", "type": ["null", "double"], "default": None},
        {"name": "mta_tax", "type": ["null", "double"], "default": None},
        {"name": "tip_amount", "type": ["null", "double"], "default": None},
        {"name": "tolls_amount", "type": ["null", "double"], "default": None},
        {"name": "improvement_surcharge", "type": ["null", "double"], "default": None},
        {"name": "total_amount", "type": ["null", "double"], "default": None},
        {"name": "payment_type", "type": ["null", "long"], "default": None},
        {"name": "congestion_surcharge", "type": ["null", "double"], "default": None},
        {"name": "taxi_type", "type": ["null", "string"], "default": None}
    ]
}

def process_and_save_data(year, month, taxi_type):

    parquet_filename = f"processed_parquet_files/{taxi_type}_tripdata_{year}-{month:02d}_processed.parquet"
    avro_filename = f"processed_avro_files/{taxi_type}_tripdata_{year}-{month:02d}_processed.avro"

    if os.path.exists(parquet_filename) and os.path.exists(avro_filename):
        print(f"Files for {taxi_type} {year}-{month:02d} already exist. Skipping processing.")
        return None  

    #load Parquet file into df
    filename = f"parquet_files/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    df = pd.read_parquet(filename)

    # convert to common schema
    if taxi_type == 'yellow':
        df = df.rename(columns={'tpep_pickup_datetime': 'pickup_datetime', 'tpep_dropoff_datetime': 'dropoff_datetime'})
    else: 
        df = df.rename(columns={'lpep_pickup_datetime': 'pickup_datetime', 'lpep_dropoff_datetime': 'dropoff_datetime'})

    df['taxi_type'] = taxi_type

    df = df.astype({
        'VendorID': 'Int64',
        'pickup_datetime': 'datetime64[ns]',
        'dropoff_datetime': 'datetime64[ns]',
        'store_and_fwd_flag': 'object',
        'RatecodeID': 'Int64',
        'PULocationID': 'Int64',
        'DOLocationID': 'Int64',
        'passenger_count': 'Int64',
        'trip_distance': 'float64',
        'fare_amount': 'float64',
        'extra': 'float64',
        'mta_tax': 'float64',
        'tip_amount': 'float64',
        'tolls_amount': 'float64',
        'improvement_surcharge': 'float64',
        'total_amount': 'float64',
        'payment_type': 'Int64',
        'congestion_surcharge': 'float64',
        'taxi_type': 'object'
    })

    # Save as Parquet
    df.to_parquet(parquet_filename, schema=parquet_schema)

    # Save as Avro
    records = df.to_dict(orient='records')
    with open(avro_filename, 'wb') as out:
        fastavro.writer(out, fastavro.parse_schema(avro_schema), records)

    return df

def process_all_data(start_year, end_year, start_month=1, end_month=12, taxi_types=['yellow', 'green']):
    years = range(start_year, end_year + 1)
    months = range(start_month, end_month + 1)
    latest_data = None
    missing_dates = []

    for year in years:
        for month in months:
            for taxi_type in taxi_types:
                try:
                    if not download_tlc_data(year, month, taxi_type):
                        missing_dates.append((year, month, taxi_type))
                        print(f"Data for {taxi_type} {year}-{month:02d} is not available. Stopping further iterations.")
                        print(f"Latest data found up to: {latest_data}")
                        print(f"Missing/Unavailable dates: {missing_dates}")
                        return
                    process_and_save_data(year, month, taxi_type)
                    latest_data = (year, month)
                except Exception as e:
                    print(f"Failed to process data for {taxi_type} {year}-{month:02d}: {e}")

    print(f"Latest data found up to: {latest_data}")
    print(f"Missing/Unavailable dates: {missing_dates}")


if __name__ == "__main__":

    start_year = 2022
    end_year = datetime.now().year
    start_month = 1
    end_month = datetime.now().month

    process_all_data(start_year, end_year, start_month, end_month)
