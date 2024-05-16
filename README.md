# NYC Taxi Data Pipeline

## Overview

This project is designed to create a data pipeline for NYC taxi trip data. The pipeline automatically downloads, processes, analyzes, and visualizes data for both yellow and green taxis from January 2022 onwards. The pipeline is structured into four main components:

1. **Download Data:** Downloads the latest available datasets.
2. **Process Data:** Processes and saves the data in Parquet and Avro formats.
3. **Analyze Data:** Analyzes the processed data to generate insights.
4. **Plot Data:** Visualizes the analysis results.

## To Manually run
```bash
python process_data.py
python analyze.py
python plot_graphs.py
```
## Components

### 1. Download Data

The `download_data.py` script handles downloading the datasets from the NYC taxi trip data repository. It checks if the file already exists to avoid redundant downloads. <br>
It is called from process_data.py so whenever new data is downloaded it is automatically processed.

### 2. Process Data

The `process_data.py` script processes the downloaded datasets, converts them to a common schema, and saves them in both Parquet and Avro formats.

### 3. Analyze Data

The `analyze.py` script analyzes the processed data to generate insights such as the average distance driven per hour, the day with the lowest number of single rider trips, and the top 3 busiest hours.

### 4. Plot Data

The `plot_graphs.py` script generates visualizations of the analysis results, including plots of the average distance driven per hour, the day with the lowest number of single rider trips, and the top 3 busiest hours.


## Automation with Cron Job

To ensure the data pipeline runs automatically and processes the latest data every month, we will use a cron job. Below is an example of a cron file (`crontab`):

**Cron Job Example:**

```cron
0 0 1 * * /usr/bin/python3 /home/Venture_data/process_data.py
```

This cron job will run the `process_data.py` script at midnight on the first day of every month.

## Directory Structure

The project directory structure is as follows:

```
NYC-Taxi-Data-Pipeline/
├── download_data.py
├── process_data.py
├── analyze.py
├── plot_graphs.py
├── parquet_files/
├── processed_parquet_files/
├── processed_avro_files/
├── output_files/
└── README.md
```

## Requirements

Ensure you have the following Python packages installed:

- pandas
- pyarrow
- fastavro
- matplotlib
- requests

You can install these packages using pip:

```bash
pip install pandas pyarrow fastavro matplotlib requests
```

## Conclusion

This data pipeline automates the process of downloading, processing, analyzing, and visualizing NYC taxi trip data. With the help of a cron job, the pipeline ensures that the latest data is processed every month, providing up-to-date insights and visualizations.
