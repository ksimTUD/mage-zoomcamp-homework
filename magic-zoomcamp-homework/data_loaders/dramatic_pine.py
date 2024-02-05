import io
import pandas as pd
import requests
from datetime import datetime, timedelta

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_apicurrent(*args, **kwargs):
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/'

    fname = 'green_tripdata_*.csv.gz'
    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(), #
                    'passenger_count': pd.Int64Dtype(), #
                    'trip_distance': float, #
                    'RatecodeID':pd.Int64Dtype(), #
                    'store_and_fwd_flag':str, #
                    'PULocationID':pd.Int64Dtype(), #
                    'DOLocationID':pd.Int64Dtype(), #
                    'payment_type': pd.Int64Dtype(), #
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'congestion_surcharge':float,
                    'trip_type': pd.Int64Dtype(),
                    'ehail_fee': float,
                }

    start_date = datetime(2020, 10, 1)  # Adjust the start date as needed
    end_date = datetime(2020, 12, 1)  # Adjust the end date as needed

    # native date parsing 
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    all_dfs = []  # List to store each month's DataFrame
    current_date = start_date
    
    while current_date <= end_date:
        # Format the URL for the current month's data file
        formatted_date = current_date.strftime('%Y-%m')
        file_url = f"{url}/green_tripdata_{formatted_date}.csv.gz"
        #  https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
        
        # Attempt to read the .csv.gz file directly into a pandas DataFrame
        try:
            df = pd.read_csv(file_url, sep=',', dtype=taxi_dtypes, parse_dates=parse_dates, compression='gzip')
            all_dfs.append(df)
            print(f"Loaded data for {formatted_date}")
        except Exception as e:
            print(f"Could not load data for {formatted_date}: {e}")
        
        # Move to the next month
        current_date += timedelta(days=32)
        current_date = current_date.replace(day=1)
    
    # Concatenate all DataFrames into one
    final_df = pd.concat(all_dfs, ignore_index=True)

    return pd.concat(all_dfs, ignore_index=True)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'