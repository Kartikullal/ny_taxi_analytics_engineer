from pathlib import Path 
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries = 3, cache_key_fn=task_input_hash)
def fetch(dataset_url : str) -> pd.DataFrame:
    """Read data from web into pandas Dataframe"""

    df = pd.read_parquet(dataset_url)

    return df


@task(log_prints = True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix some Dtype issues"""

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df = df.drop_duplicates().reset_index(drop=True)
    df['trip_id'] = df.index + 1
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file : str) -> Path:
    """Write dataframe out locally as a parquet file"""

    path = Path(f"./data/ny_taxi/{color}/{dataset_file}.parquet")
    print(path)

    return path


@task()
def write_gcs(path : Path, df: pd.DataFrame) -> None:
    """Uploading local parquet file to gcs"""

    gcs_block = GcsBucket.load("de-projects-gcs")
    gcs_block.upload_from_dataframe(
        df = df,
        to_path = path,
        serialization_format= "parquet"
    )

    return 
@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL Function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"


    df = fetch(dataset_url)
    df_clean = clean(df)

    path = write_local(df_clean, color, dataset_file)

    write_gcs(path, df_clean)

@flow()
def etl_web_to_gcs_parent_flow(
    months: list = [1, 2], years: list = [2021], color: str = "yellow"
):
    for year in years:
        for month in months:
            etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    color = "yellow"
    months = [4]
    year = [2020]
    etl_web_to_gcs_parent_flow(months, year, color)