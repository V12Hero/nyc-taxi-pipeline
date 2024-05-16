import pandas as pd
os.makedirs('output_files', exist_ok=True)

def perform_analysis():
    all_data = []
    # Load processed Parquet files
    for year in range(2022, datetime.now().year + 1):
        for month in range(1, 13):
            for taxi_type in ['yellow', 'green']:
                parquet_filename = f"processed_parquet_files/{taxi_type}_tripdata_{year}-{month:02d}_processed.parquet"
                if os.path.exists(parquet_filename):
                    df = pd.read_parquet(parquet_filename)
                    all_data.append(df)
                else:
                    print(f"Missing file: {parquet_filename}")

    if all_data:
        data = pd.concat(all_data, ignore_index=True)
        data['pickup_datetime'] = pd.to_datetime(data['pickup_datetime'])
        data['hour'] = data['pickup_datetime'].dt.hour
        data['day_of_week'] = data['pickup_datetime'].dt.dayofweek
        
        # Average dist by yellow and green taxis per hour
        avg_distance_per_hour = data.groupby(['taxi_type', 'hour'])['trip_distance'].mean().reset_index()
        avg_distance_per_hour.to_csv('output_files/avg_distance_per_hour.csv', index=False)
        
        # Day with the lowest number of single rider trips
        single_rider_trips = data[data['passenger_count'] == 1]
        lowest_single_rider_day = single_rider_trips.groupby('day_of_week').size().idxmin()
        with open('output_files/lowest_single_rider_day.txt', 'w') as f:
            f.write(str(lowest_single_rider_day))
        
        # Top 3 busiest hours
        busiest_hours = data.groupby('hour').size().nlargest(3).reset_index(name='count')
        busiest_hours.to_csv('output_files/busiest_hours.csv', index=False)
    else:
        print("No data to analyze")

if __name__ == "__main__":
    perform_analysis()
