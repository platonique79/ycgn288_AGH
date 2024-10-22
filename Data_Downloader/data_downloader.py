from datetime import datetime,timedelta
from warnings import catch_warnings
import os

import pandas as pd
import logging
import yfinance as yf
import pandas_market_calendars as mcal

from google.cloud import storage

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def fetch_sp500_tickers():
    logging.info("Fetching SP500 tickers...")
    # Example subset of SP500 tickers
    sp500_tickers = ['AAPL', 'MSFT', 'GOOG', 'AMZN', 'TSLA', 'BRK-B', 'JNJ', 'V', 'NVDA']
    logging.info(f"Fetched {len(sp500_tickers)} tickers.")
    return sp500_tickers

#Function to get the list of filenames already downloaded
def get_gstorage_data_downloaded(bucket_name,project_id):
    logging.info("Getting data already downloaded to google storage...")
    #fetch the downloaded data filenames
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs()
    fdates = []
    for blob in blobs:
        fdate = blob.name[-14:-4]
        fdates.append(fdate)
    return fdates

def get_local_data_downloaded(local_path):
    logging.info(f"Getting data already downloaded to local storage {local_path}...")
    files = os.listdir(local_path)
    # Filtering only the files.
    files = [f for f in files if os.path.isfile(local_path + '/' + f)]
    fdates = []
    for file in files:
        fdate = file[-14:-4]
        fdates.append(fdate)
    return fdates

def get_trading_days(start,end):
    nyse = mcal.get_calendar('NYSE')
    trading_days = nyse.schedule(start_date=start, end_date=end)
    return trading_days

def create_bucket(bucket_name,project_id, location="northamerica-northeast1"):
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    # Create storage bucket if it does not exist
    if not bucket.exists():
        new_bucket = storage_client.create_bucket(bucket, location=location)
        logging.info(f"Bucket {new_bucket.name} created in location {new_bucket.location}.")
    else:
        logging.info(f"Bucket {bucket_name} already exists.")

def create_df_to_gstorage(bucket_name,project_id,df,filename):
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    bucket.blob(filename).upload_from_string(df.to_csv(index=False), 'text/csv')


def main(days,local=None):
    #Check if a local path is passed to the main function
    if local:
        #Check if local is a valid folder and it exists otherwise the code will fail
        if os.path.isdir(local):
            logging.info(f"Downloading S&P500 data for the past {days} days...")
        else:
            print(f"Invalid path: {local}, or folder doesn't exist")
            exit(1)
        logging.info("Local storage is selected.")
        #Get the dates of local files already downloaded
        filedates = get_local_data_downloaded(local)
    else:
        logging.info("Google storage is selected.")
        project_id = "agh-ycgn288-sp500-project"
        bucket_name = "agh_sp500"
        #Create a new bucket if it doesn't exist
        create_bucket(bucket_name, project_id, location="northamerica-northeast1")
        # Get the dates of Google bucket files already downloaded
        filedates = get_gstorage_data_downloaded(bucket_name, project_id)






    start_date = datetime.today() - timedelta(days=days)
    end_date = datetime.today()

    #Get a dataframe for trading days
    trading_days = get_trading_days(start_date,end_date)


    #Loop for the number of  past days
    for i in range(days):
        from_date = start_date + timedelta(days=i)
        to_date= from_date + timedelta(days=1)

    ## check if it's a trading day
        if from_date.strftime("%Y-%m-%d") in trading_days.index:
            # download the data if the file doesn't exist
            if from_date.strftime("%Y-%m-%d") not in filedates:
                logging.info(f"Downloading data for {from_date.strftime('%Y-%m-%d')}...")
                data=yf.download(tickers=fetch_sp500_tickers(), start=from_date.strftime("%Y-%m-%d"), end=to_date.strftime("%Y-%m-%d"))
                df = pd.DataFrame(data)

                fname = 'AGH_sp500_' + from_date.strftime('%Y-%m-%d') + '.csv'

                if local:
                    fname=local + fname
                    print(df.to_csv(path_or_buf=fname, index=False))
                else:
                    create_df_to_gstorage(bucket_name,project_id,df,fname)
                logging.info(f"DATA DOWNLOADED successfully stored for {from_date.strftime('%Y-%m-%d')}.")
            else:
                logging.info(f"Data for {from_date.strftime('%Y-%m-%d')} already downloaded.")
        else:
            logging.info(f"DOWNLOAD ERROR - {from_date.strftime('%Y-%m-%d')} is not a trading day, No data was downloaded for this date." )

if __name__ == "__main__":
    #main(9, './agh_sp500_data_files/')
    main(9)