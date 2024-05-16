import pandas as pd
import matplotlib.pyplot as plt

def plot_avg_distance_per_hour():
    data = pd.read_csv('output_files/avg_distance_per_hour.csv')
    plt.figure(figsize=(10, 6))
    for taxi_type in data['taxi_type'].unique():
        subset = data[data['taxi_type'] == taxi_type]
        plt.plot(subset['hour'], subset['trip_distance'], label=f'{taxi_type.capitalize()} Taxi')
    plt.xlabel('Hour of Day')
    plt.ylabel('Average Distance (miles)')
    plt.title('Average Distance Driven per Hour')
    plt.legend()
    plt.grid(True)
    plt.savefig('output_files/avg_distance_per_hour.png')
    plt.show()

def plot_lowest_single_rider_day():
    with open('output_files/lowest_single_rider_day.txt', 'r') as f:
        lowest_single_rider_day = int(f.read().strip())
    
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_counts = pd.Series([0]*7)
    day_counts[lowest_single_rider_day] = 1  

    plt.figure(figsize=(10, 6))
    plt.bar(day_names, day_counts, color='lightgreen')
    plt.xlabel('Day of Week')
    plt.ylabel('Number of Single Rider Trips')
    plt.title('Day with the Lowest Number of Single Rider Trips')
    plt.savefig('output_files/lowest_single_rider_day.png')
    plt.show()

def plot_busiest_hours():
    data = pd.read_csv('output_files/busiest_hours.csv')
    plt.figure(figsize=(10, 6))
    plt.bar(data['hour'], data['count'], color='skyblue')
    plt.xlabel('Hour of Day')
    plt.ylabel('Number of Trips')
    plt.title('Top 3 Busiest Hours')
    plt.grid(True)
    plt.savefig('output_files/busiest_hours.png')
    plt.show()

if __name__ == "__main__":
    plot_avg_distance_per_hour()
    plot_lowest_single_rider_day()
    plot_busiest_hours()
