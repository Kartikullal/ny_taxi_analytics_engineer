from pathlib import Path 
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import cloud_storage_download_blob_as_bytes

#
# https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2023-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2023-01.parquet

@task()
def extract_from_gcs(color: str, year : int, month : int) -> Path:
    """ Download trip data from gcs bucket"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}.parquet"
    gcs_path = f"data/ny_taxi/{color}/{dataset_file}"

    gcs_block = GcsBucket.load("de-projects-gcs")
    gcp_credentials_block = GcpCredentials.load("de-project-gcp-creds")
    contents = cloud_storage_download_blob_as_bytes(
                bucket=gcs_block
                , blob = gcs_path
                , gcp_credentials=gcp_credentials_block)
    print(contents)

    return Path(f"../data/{gcs_path}")


@flow()
def etl_gcs_to_bq():
    """Main etl flow to lead data into bigquery"""

    color = "yellow"
    year = 2021
    month = 1
    

    path = extract_from_gcs(color, year, month)
    # df = transform(path)
    # write_bq(df)

if __name__ == '__main__':
    etl_gcs_to_bq()