# airline-bigquery-spark-ETL

## Project Overview

This project is an example of extracting, transforming, and loading (ETL) the OpenSky dataset into BigQuery using PySpark and Airflow.

The [OpenSky dataset]('https://zenodo.org/record/4601479#.Y8PJwnZBxPZ') includes information on flight number, location, velocity, and other parameters for each aircraft tracked by the OpenSky Network. The dataset is open-sourced and can be used for research and educational purposes.

The PySpark pipeline in this project extracts the data from the dataset, performs any necessary transformations, and loads the data into BigQuery as a fact table. The pipeline is run by an Airflow DAG that can be scheduled to run on a regular interval, such as daily.

The columns available in the OpenSky dataset are:

- `callsign`: the identifier of the flight displayed on ATC screens (usually the first three letters are reserved for an airline: AFR for Air France, DLH for Lufthansa, etc.)
- `number`: the commercial number of the flight, when available (the matching with the callsign comes from public open API)
- `icao24`: the transponder unique identification number;
- `registration`: the aircraft tail number (when available);
- `typecode`: the aircraft model type (when available);
- `origin`: a four letter code for the origin airport of the flight (when available);
- `destination`: a four letter code for the destination airport of the flight (when available);
- `firstseen`: the UTC timestamp of the first message received by the OpenSky Network;
- `lastseen`: the UTC timestamp of the last message received by the OpenSky Network;
- `day`: the UTC day of the last message received by the OpenSky Network;
- `latitude_1`: the first detected latitude of the aircraft;
- `longitude_1`: the first detected longitude of the aircraft.
- `altitude_1`: the first detected altitude of the aircraft;
- `latitude_2`: the last detected latitude of the aircraft;
- `longitude_2`: the last detected longitude of the aircraft.
- `altitude_2`: the last detected altitude of the aircraft;

This project is intended as an example and may need to be adjusted to fit specific use cases.

## Tech Stack

- PySpark
- Airflow
- BigQuery

## How to use

1. Clone this repository
2. Replace the path to the OpenSky dataset and the table name in the PySpark pipeline and DAG
3. Set up the Airflow environment and import the DAG
4. Run the DAG
5. Verify the data in the BigQuery table.

You will also need to have the BigQuery connector for spark installed and also the credentials to access your project.
