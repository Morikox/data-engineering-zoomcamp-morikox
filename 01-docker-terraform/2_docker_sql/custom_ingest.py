
import argparse
import pandas as pd
import gzip
import csv
import os

from sqlalchemy import create_engine
from time import time

def main(params):

    os.chdir(r'c:\\Users\\hp\\Desktop\\Learning Data\\Data Engineering\\zoomcamp 2024\\data-engineering-zoomcamp-morikox\\01-docker-terraform\\2_docker_sql')
    
    user = params.users
    password = params.passwords
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table
    gz_file_path = 'yellow_tripdata_2021-01.csv.gz'
    output_csv_path = 'ytrip_data_2021_01.csv'

    with gzip.open(gz_file_path, 'rt', encoding='utf-8') as gz_file:
        with open(output_csv_path, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            
            for line in gz_file:
            
                csv_writer.writerow(line.strip().split(',')) 

    print(f"CSV file extracted from {gz_file_path} and saved to {output_csv_path}.")


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df_iter = pd.read_csv(output_csv_path, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    while True:
        try:
            t_start = time()
            df = next(df_iter)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists='append', index = False)
            t_end = time()
            print('Inserted another chunk, took %.3f seconds' % (t_end - t_start))
        except StopIteration:
            print("Iteration completed successfully.")
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            # Handle the error as needed, or remove the except block if you want to exit on any exception
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Ingesting CSV data to Postgres DB')

    parser.add_argument('--users', help = 'username for postgres db')
    parser.add_argument('--passwords', help = 'passwrod for postgres db')
    parser.add_argument('--host', help = 'host name for postgres db')
    parser.add_argument('--port', help = 'port number for postgres db')
    parser.add_argument('--db', help = 'db name for postgres db')
    parser.add_argument('--table', help = 'table name for postgres db')

    args = parser.parse_args()
    main(args)





