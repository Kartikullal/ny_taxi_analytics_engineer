from pathlib import Path 
import pandas as pd
import io
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import cloud_storage_download_blob_as_bytes


# @task(retries = 3)
def extract_from_gcs(color: str, year : int, month : int) -> Path:
    """ Download trip data from gcs bucket"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}.parquet"
    gcs_path = f"data/ny_taxi/{color}/{dataset_file}"

    gcs_block = GcsBucket.load("de-projects-gcs")
    gcp_credentials_block = GcpCredentials.load("de-project-gcp-creds")
    blob_bytes = cloud_storage_download_blob_as_bytes(
                bucket='de_projects_kartik'
                , blob = gcs_path
                , gcp_credentials=gcp_credentials_block)
    df = pd.read_parquet(io.BytesIO(blob_bytes))
    
    print(df.columns)
    return df
#https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2020-01.parquet

@task(retries = 3)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """ Data Cleaning"""
    # print(path)
    # df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    # df['fare_amount'] = df['fare_amount'].abs()

    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")

    return df


@task()
def write_bq(df : pd.DataFrame, color: str) -> None:
    """Write Dataframe to Bigquery"""

    gcp_credentials_block = GcpCredentials.load("de-project-gcp-creds")
    df.to_gbq(
        destination_table=f"ny_taxi.{color}_taxi_rides",
        project_id="bubbly-domain-408520",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='append'

    )

@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:
    """Main etl flow to lead data into bigquery"""

    df = extract_from_gcs(color, year, month)
    if (color in ['yellow','green']):
        df = transform(df)
    write_bq(df,color)


@flow()
def etl_gcs_to_bq_parent_flow(
    months: list = [1, 2], years: list = [2021], color: str = "yellow"
):
    for year in years:
        for month in months:
            etl_gcs_to_bq(year, month, color)


if __name__ == "__main__":
    color = "yellow"
    months = [12]
    year = [2021,2022]
    etl_gcs_to_bq_parent_flow(months, year, color)