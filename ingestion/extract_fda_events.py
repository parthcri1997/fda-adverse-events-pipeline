#Pulling data from API for bronze layer and storing the raw files on S3 bucket.


import requests
from dotenv import load_dotenv
import os
import json
import boto3
import datetime
#Load environment variables from .env file
load_dotenv()


class BronzeLayer:
    def __init__(self):
        self.bucket_name = "fda-events"
        self.s3_client = boto3.client('s3')
        self.FDA_API_KEY = os.getenv("FDA_API_KEY")
        self.api_limit = 1000
        
    def get_data_from_api(self):
        url = f"https://api.fda.gov/drug/event.json?limit={str(self.api_limit)}&sort=receivedate:asc"
        page_number = 1
        is_uploaded = False
        #Initial API Request - No skip parameter as only 26000 hits can be done as per https://open.fda.gov/apis/paging/, sorted by receivedate ascending for using the search_after feature useful for retrieving results as paginated records.
        response = requests.get(url,params={"api_key":self.FDA_API_KEY})
        if response.status_code!=200:
            return None
        data = response.json()
        is_uploaded = self.put_files_on_s3(data=data,page_number=page_number)
        if not is_uploaded:
            return None
        page_number+=1
        # Link header contains next page URL; loop exits when header is absent
        next_link = response.headers.get("Link")
        # Extract next page URL from Link response header — None indicates last page
        while next_link is not None:
            # Parse next URL from Link header — strip angle brackets and rel="next"
            new_url = next_link[1:].split(">;")[0]
            response = requests.get(new_url,params={"api_key":self.FDA_API_KEY})
            if response.status_code!=200:
                break
            data = response.json()
            # Only upload to S3 if results are present in response
            if len(data["results"]) > 0:
                is_uploaded = self.put_files_on_s3(data=data,page_number=page_number)
                if not is_uploaded:
                    break
            next_link = response.headers.get("Link")
            page_number+=1
        
        return True

    def put_files_on_s3(self,data,page_number):
        # Attempt S3 upload with retry logic (max 3 attempts)
        # S3 key follows medallion bronze layer partitioning: dt=YYYY-MM-DD/page_XXXX.json
        max_retries = 3
        for attempt in range(max_retries):
            s3_response = self.s3_client.put_object(Body = json.dumps(data["results"]),Bucket=self.bucket_name,Key=f"bronze/fda_drug_events/dt={str(datetime.date.today())}/page_{str(page_number).zfill(4)}.json")
            if s3_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                # Return True on successful upload, retry on failure
                return True
            print(f"Retry {attempt+1} for page {page_number}")
        print(f"Failed to upload page {page_number} after {max_retries} attempts.")
        # All retries exhausted — log failure and return False
        return False
        


def run_bronze_ingestion():
    # Entry point called by Airflow PythonOperator. To be added in airflow dags later.
    bronze = BronzeLayer()
    result = bronze.get_data_from_api()
    if result:
        print("Ingestion completed successfully.")
    else:
        print("Ingestion failed.")

if __name__== "__main__":
    run_bronze_ingestion()
