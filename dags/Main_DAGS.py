# Main Libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator, BigQueryDeleteDatasetOperator,
    BigQueryCreateExternalTableOperator, BigQueryExecuteQueryOperator,
    BigQueryCreateEmptyDatasetOperator, BigQueryDeleteTableOperator
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

import pandas as pd
import os
import ast
from typing import Dict
from datetime import datetime
from pathlib import Path


# –––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

# First DAG – Cleaning Data

# (Step №1) Reading main data from API
def read_data(wine_path: str, wineries_path: str,
              rating_path: str, harmonize_path: str) -> Dict[str, pd.DataFrame]:
    """
    Reads JSON data from specified paths and converts it into pandas DataFrames.

    Parameters:
        wine_path: The JSON file with wine data.
        wineries_path: The JSON file with wineries data.
        rating_path: The JSON file with ratings data.
        harmonize_path: The JSON file with harmonize images data(URL).

    Returns:
        A dictionary with keys 'wines_data', 'wineries_data', 'ratings_data',
        and 'harmonize_images_data', each mapping to a corresponding DataFrame.

    Example:
        dataset_dict = read_data('wine.json', 'wineries.json', 'ratings.json', 'harmonize.json')
    """

    # Read Wines endpoint
    wines = pd.read_json(wine_path)
    # Change col names in Wine dataset
    wines.columns = wines.columns.str.title().str.replace('id$', 'ID', regex=True) \
        .str.replace('name$', 'Name', regex=True)
    wines = wines.rename(columns={'Abv': 'ABV', 'Url': 'wine_url'})

    # Read Wineries endpoint
    wineries = pd.read_json(wineries_path)
    # Change col names in Wineries dataset
    wineries.columns = wineries.columns.str.title().str.replace('id$', 'ID', regex=True) \
        .str.replace('name$', 'Name', regex=True)

    # Read Rating endpoint
    rating = pd.read_json(rating_path)
    # Change col names in Ratings dataset
    rating.columns = rating.columns.str.title().str.replace('id$', 'ID', regex=True)

    # Read Harmonize_Images endpoint
    harmonize_images = pd.read_json(harmonize_path)
    # Change col names in Harmonize_Images dataset
    harmonize_images = harmonize_images.rename(columns={'snacks': 'Harmonize', 'url': 'harmonize_url'})

    # Create dict of datasets
    dict_datasets = {'wines_data': wines,
                     'wineries_data': wineries,
                     'ratings_data': rating,
                     'harmonize_images_data': harmonize_images}

    return dict_datasets


# (Step №2) Creating dataset for Harmonize
def harmonize_db_creation(**kwargs) -> Dict[str, pd.DataFrame]:
    """
    Creates harmonizer datasets from wine and image data.

    Parameters:
        **kwargs: Arguments from Airflow's XCom, typically including wine and image data.

    Returns:
        A dictionary with two DataFrames: 'wine_id' (harmonizer data with wine IDs)
        and 'no_wine_id' (harmonized data excluding wine IDs).

    Example:
        harmonized_data = harmonize_db_creation(ti=xcom_instance)
    """

    # Create instance for Xcom
    ti = kwargs['ti']

    # Get data from first task

    dict_datasets = ti.xcom_pull(task_ids='read_data')
    # Read Wine dataset
    df = dict_datasets['wines_data']
    # Read Harmonize_Photo dataset
    df_photo = dict_datasets['harmonize_images_data']

    # Create a copy of a frame for Harmonize data
    unnested_harmonize = df[['Harmonize', 'WineID']].copy()

    # Convert string to list type
    unnested_harmonize['Harmonize'] = unnested_harmonize['Harmonize'].apply(lambda x: ast.literal_eval(x))

    # Explode list-values
    unnested_harmonize = unnested_harmonize.explode('Harmonize')

    # Get unique harmonize-values
    unique_harmonize = unnested_harmonize['Harmonize'].unique()

    # Create ID for unique harmonize
    harmonize_db = pd.DataFrame()
    harmonize_db['HarmonizeID'] = list(range(1000001, 1000001 + len(unique_harmonize)))
    harmonize_db['Harmonize'] = unique_harmonize

    # Merge with unique harmonizers
    harmonize_db = pd.merge(harmonize_db, unnested_harmonize, on='Harmonize')

    # Add photo url
    harmonize_db = pd.merge(harmonize_db, df_photo, on='Harmonize')

    # Copy frame to save WineID
    harmonize_db_wineid = harmonize_db.copy()

    # Delete redundant column and get unique values
    if 'WineID' in harmonize_db.columns:
        harmonize_db = harmonize_db.drop(columns={'WineID'}).drop_duplicates()

    # Create dictionary for Harmonize data
    dict_harmonize: Dict[str, pd.DataFrame] = {'wine_id': harmonize_db_wineid,
                                               'no_wine_id': harmonize_db}

    return dict_harmonize


# (Step №3) Creating dataset for Joined_table
def joined_table_db_creation(**kwargs) -> pd.DataFrame:
    """
    Creates a joined table by merging wine and region data with harmonizer data (merging their id's).

    Parameters:
        **kwargs: Arguments from Airflow's XCom, used to pull wine and harmonized data.

    Returns:
        A DataFrame representing the joined table, combining Wine, Region, and Harmonize data.

    Example:
        joined_table = joined_table_db_creation(ti=xcom_instance)
    """

    # Create instance for Xcom
    ti = kwargs['ti']

    # Get data from first task
    dict_datasets = ti.xcom_pull(task_ids='read_data')
    # Read Wine dataset
    df_wine = dict_datasets['wines_data']

    # Get data from second task
    dict_harmonize = ti.xcom_pull(task_ids='create_harmonize_db')
    # Get Harmonize dataset with WineID
    df_harmonize = dict_harmonize['wine_id']

    # Join Wine & Region & Harmonize
    joined_table_db = df_wine[['WineID', 'RegionID', 'WineryID']].merge(df_harmonize[['HarmonizeID', 'WineID']],
                                                                        on='WineID')

    return joined_table_db


# (Step №5) Creating dataset for Regions
def region_db_creation(**kwargs) -> pd.DataFrame:
    """
    Extracts and creates a unique regions dataset from wine data.

    Parameters:
        **kwargs: Keyword arguments for XCom pull in Airflow to access wine data.

    Returns:
        A DataFrame with unique regions, including 'RegionID' and 'RegionName'.

    Example:
        regions_table = region_db_creation(ti=xcom_instance)
    """

    # Create instance for Xcom
    ti = kwargs['ti']

    # Get data from first task
    dict_datasets = ti.xcom_pull(task_ids='read_data')
    # Read Wine dataset
    df = dict_datasets['wines_data']

    # Get needed columns & unique values
    regions_db = df[['RegionID', 'RegionName']].drop_duplicates()

    return regions_db


# (Step №6) Creating dataset for Winery
def wineries_db_creation(**kwargs) -> pd.DataFrame:
    """
    Generates a DataFrame of unique wineries from provided data.

    Parameters:
        **kwargs: Keyword arguments for XCom pull in Airflow, to access wineries data.

    Returns:
        A DataFrame containing distinct 'WineryID' and 'WineryName'.

    Example:
        wineries_table = wineries_db_creation(ti=xcom_instance)
    """

    # Create instance for Xcom
    ti = kwargs['ti']

    # Get data from first task
    dict_datasets = ti.xcom_pull(task_ids='read_data')
    # Read Wine dataset
    df = dict_datasets['wineries_data']

    # Get needed columns & unique values
    regions_db = df[['WineryID', 'WineryName']].drop_duplicates()

    return regions_db


# (Step №7) Creating dataset for Wines
def wines_db_creation(**kwargs) -> pd.DataFrame:
    """
    Creates a refined DataFrame of wines by merging and cleaning data.

    Parameters:
        **kwargs: Keyword arguments for XCom pull in Airflow, used to fetch wine and ratings data.

    Returns:
        A DataFrame of wines, enhanced with average ratings and cleaned of redundant columns.

    Example:
        wines_table = wines_db_creation(ti=xcom_instance)
    """

    # Create instance for Xcom
    ti = kwargs['ti']

    # Get data from first task
    dict_datasets = ti.xcom_pull(task_ids='read_data')
    # Read Wine dataset
    df_wine = dict_datasets['wines_data']
    # Read Ratings dataset
    df_ratings = dict_datasets['ratings_data']

    # Merge Avg_Rating data
    df_wine = df_wine.merge(df_ratings, on='WineID')

    # Delete redundant columns
    redund_cols = ['RegionID', 'RegionName', 'Harmonize', 'Code', 'Grapes', 'WineryID']

    if any(col in df_wine.columns for col in redund_cols):
        wines_db = df_wine.drop(redund_cols, axis=1)

    return wines_db


# (Step №8) Save datasets as csv in a folder
def save_datasets(**kwargs) -> None:
    """
    Saves various dataframes as CSV files in a cleaned_datasets folder.

    Parameters:
        **kwargs: Keyword arguments from Airflow's XCom to access different dataframes.

    Functionality:
        - Retrieves dataframes for harmonized data, regions, wineries, wines, and joined tables.
        - Creates a folder named 'cleaned_datasets' if it doesn't exist.
        - Saves each dataframe as a CSV file in the 'cleaned_datasets' folder.

    Example:
        save_datasets(ti=xcom_instance)
    """

    # Create instance for Xcom
    ti = kwargs['ti']

    # Get data from second task
    dict_harmonize = ti.xcom_pull(task_ids='create_harmonize_db')
    # Read Harmonize table with no WineID
    df_harmonize = dict_harmonize['no_wine_id']

    # Read Region table
    df_region = ti.xcom_pull(task_ids='create_region_db')
    # Read Winery table
    df_wineries = ti.xcom_pull(task_ids='create_wineries_db')
    # Read Wine table
    df_wines = ti.xcom_pull(task_ids='create_wines_db')
    # Read Join Table
    df_joined = ti.xcom_pull(task_ids='create_joined_table_db')

    # Create folder
    folder_path = "/opt/airflow/data/cleaned_data"
    os.makedirs(folder_path, exist_ok=True)

    # Saving files to folder
    file_names = ['regions_db.csv', 'wineries_db.csv', 'harmonize_db.csv',
                  'wines_db.csv', 'joined_table_db.csv']

    datasets = [df_region, df_wineries, df_harmonize, df_wines, df_joined]

    for name, data in zip(file_names, datasets):
        data.to_csv(os.path.join(folder_path, name), index=False)


# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––--------------

# Bash command for Wine data
curl_command_wine = "curl -X GET 'https://apide.fly.dev/wine' -o /opt/airflow/data/wine.json"

# Bash command for Wineries data
curl_command_wineries = "curl -X GET 'https://apide.fly.dev/wineries' -o /opt/airflow/data/wineries.json"

# Bash command for Ratings data
curl_command_ratings = "curl -X GET 'https://apide.fly.dev/ratings' -o /opt/airflow/data/ratings.json"

# Bash command for Harmonize data
curl_command_harmonize = "curl -X GET 'https://apide.fly.dev/harmonizer' -o /opt/airflow/data/harmonize.json"

# Description of a First Dag
with DAG(
        "cleaning_data_final",
        description="Load data from API, clean data, save data locally",
        doc_md=__doc__,
        schedule_interval=None,
        start_date=datetime(2021, 1, 30),
        catchup=False,
) as dag1:
    with TaskGroup("loading_api_data") as loading_api_data:
        """
        #### Task 1 – 'Loading data from API'
        Using Bash Operators data converted to json file and saved in '/opt/airflow/data/' path
        """
        # Task №1.1 – Get Wine data API
        curl_task_wine = BashOperator(
            task_id='curl_api_request_wine',
            bash_command=curl_command_wine,
            dag=dag1,
        )

        # Task №1.2 – Get Wineries data API
        curl_task_wineries = BashOperator(
            task_id='curl_api_request_wineries',
            bash_command=curl_command_wineries,
            dag=dag1,
        )

        # Task №1.3 – Get Ratings data API
        curl_task_ratings = BashOperator(
            task_id='curl_api_request_ratings',
            bash_command=curl_command_ratings,
            dag=dag1,
        )

        # Task №1.4 – Get Harmonize data API
        curl_task_harmonize = BashOperator(
            task_id='curl_api_request_harmonize',
            bash_command=curl_command_harmonize,
            dag=dag1,
        )

    """
    #### Task 2 – 'Creating & cleaning main data'
        - Stored Json data is converted to pd.DataFrame
        - For each DataFrame a separate Table was created
        - Each Table was cleaned
    """

    # Task №2.1 – Downloading main data
    read_data_task = PythonOperator(
        task_id='read_data',
        python_callable=read_data,
        op_kwargs={
            'wine_path': '/opt/airflow/data/wine.json',
            'wineries_path': '/opt/airflow/data/wineries.json',
            "rating_path": '/opt/airflow/data/ratings.json',
            'harmonize_path': '/opt/airflow/data/harmonize.json'
        },
        dag=dag1,
    )

    # Task №2.2 – Creating Harmonize dataframe
    create_harmonize_task = PythonOperator(
        task_id='create_harmonize_db',
        python_callable=harmonize_db_creation,
        provide_context=True,
        dag=dag1,
    )

    # Task №2.3 – Creating Joined_table dataframe
    create_joined_table_task = PythonOperator(
        task_id='create_joined_table_db',
        python_callable=joined_table_db_creation,
        provide_context=True,
        dag=dag1,
    )

    # Task №2.4 – Creating Regions dataframe
    create_regions_table_task = PythonOperator(
        task_id='create_region_db',
        python_callable=region_db_creation,
        provide_context=True,
        dag=dag1,
    )

    # Task №2.5 – Creating Wineries dataframe
    create_wineries_table_task = PythonOperator(
        task_id='create_wineries_db',
        python_callable=wineries_db_creation,
        provide_context=True,
        dag=dag1,
    )

    # Task №2.6 – Creating Wines dataframe
    create_wines_task = PythonOperator(
        task_id='create_wines_db',
        python_callable=wines_db_creation,
        provide_context=True,
        dag=dag1,
    )

    """
    #### Task 3 – 'Saving datasets to folder'
    Each cleaned table was saved in folder – 'cleaned_data', path - "/opt/airflow/data/cleaned_data"
    """
    # Task №3 – Saving datasets to folder
    save_data_task = PythonOperator(
        task_id='save_data',
        python_callable=save_datasets,
        provide_context=True,
        dag=dag1,
    )

    """
    #### Task 4 – 'Trigger next DAG'
    Using TriggerDagRun Operator second Dag invokes
    """
    # Task №4 (Last) - Trigger next Dag
    trigger_second_dag_task = TriggerDagRunOperator(
        task_id='trigger_second_dag',
        trigger_dag_id='big_query_dashboard_final',
        dag=dag1
    )

# Execution sequence – First DAG
chain(loading_api_data, read_data_task, create_harmonize_task, create_joined_table_task,
      create_regions_table_task, create_wineries_table_task, create_wines_task, save_data_task, trigger_second_dag_task)


# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

# Second DAG – Pulling cleaned data to BigQuery

# Check existence of a dataset – 'dashboard_dataset'
def check_dataset_exists(**kwargs):
    """
    Verifies the existence of the 'dashboard_dataset' in BigQuery.

    Parameters:
        **kwargs: Keyword arguments from Airflow's XCom, allowing access to task instances and other Airflow functionalities.

    Functionality:
        - Establishes a connection to Google BigQuery using Airflow's BigQueryHook.
        - Executes an SQL query to count occurrences of the 'dashboard_dataset' within the project's datasets.
        - Determines the existence of the dataset by checking if the count is greater than 0.
        - Pushes the result (True or False) back to Airflow's XCom for use in subsequent tasks.

    Example:
        result = check_dataset_exists(ti=xcom_instance)
        # This will push True to XCom if the dataset exists, otherwise False.
    """

    # Connect to BGQ
    hook = BigQueryHook(use_legacy_sql=False, location='US')

    # Count how many do dashboard_dataset face in project
    sql = f"SELECT COUNT(schema_name) FROM `{PROJECT_ID}`.INFORMATION_SCHEMA.SCHEMATA WHERE schema_name = 'dashboard_dataset'"

    # Save Frequency
    records = hook.get_records(sql)

    # Check do the dataset_exists
    dataset_exists = records[0][0] > 0

    # Push data
    kwargs['ti'].xcom_push(key='dataset_exists', value=dataset_exists)


def branch_task(**kwargs):
    """
    Determines the next task in the workflow based on the existence of a dataset.

    Parameters:
        **kwargs: Keyword arguments from Airflow's XCom, facilitating the retrieval of data between tasks.

    Functionality:
        - Retrieves the dataset existence status previously stored in XCom by the 'check_dataset' task.
        - Based on whether the dataset exists or not, decides the subsequent task: 
          - If the dataset exists, proceeds with the 'delete_dataset' task.
          - If the dataset does not exist, proceeds with the 'create_dataset' task.

    Example:
        next_task = branch_task(ti=xcom_instance)
        # This will return either 'delete_dataset' or 'create_dataset' as the next task.
    """

    # Create instance for Xcom
    ti = kwargs['ti']

    # Get data from 'check_data' task
    dataset_exists = ti.xcom_pull(task_ids='check_dataset', key='dataset_exists')
    return 'delete_dataset' if dataset_exists else 'create_dataset'


# Defining schema for Wines Table
schema_wines = [
    {"name": "ABV", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "Acidity", "type": "STRING", "mode": "REQUIRED"},
    {"name": "Body", "type": "STRING", "mode": "REQUIRED"},
    {"name": "Country", "type": "STRING", "mode": "REQUIRED"},
    {"name": "Elaborate", "type": "STRING", "mode": "REQUIRED"},
    {"name": "Type", "type": "STRING", "mode": "REQUIRED"},
    {"name": "wine_url", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Vintages", "type": "STRING", "mode": "NULLABLE"},
    {"name": "WineID", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "WineName", "type": "STRING", "mode": "REQUIRED"},
    {"name": "Avg_Rating", "type": "FLOAT", "mode": "REQUIRED"}]

# Defining schema for Wineries Table
schema_wineries = [
    {"name": "WineryID", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "WineryName", "type": "STRING", "mode": "REQUIRED"}]

# Defining schema for Regions Table
schema_regions = [
    {"name": "RegionID", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "RegionName", "type": "STRING", "mode": "REQUIRED"}]

# Defining schema for Harmonize Table
schema_harmonize = [
    {"name": "HarmonizeID", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "Harmonize", "type": "STRING", "mode": "REQUIRED"},
    {"name": "harmonize_url", "type": "STRING", "mode": "NULLABLE"}]

# Defining schema for Joined Table
schema_joined_table = [
    {"name": "WineID", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "RegionID", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "WineryID", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "HarmonizeID", "type": "INTEGER", "mode": "REQUIRED"}]

# Defining base path
base_path = Path(__file__).parents[1]

# Defining Project ID
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

# Defining Bucket ID
gcp_bucket = os.environ.get("GCP_GCS_BUCKET")

# Defining Dataset name
bq_dataset = "dashboard_dataset"

# Defining route for Bucket
gcp_data_dest = "data/"

# Creating Second dag
with DAG(
        "big_query_dashboard_final",
        description="Pulling cleaned data to BigQuery",
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
) as dag2:
    """
    #### Task 1 – 'Check existence of a dataset'
    Using Python Operator dataset is checked for presence or absence
    """
    # Task 1 – Check dataset for presence
    check_dataset = PythonOperator(
        task_id='check_dataset',
        python_callable=check_dataset_exists,
        provide_context=True,
        dag=dag2
    )

    """
    #### Task 2 – 'Branch conditioning'
    Using BranchPython Operator two paths of execution are created:
        - Path 1: create dataset & upload data & load to GBQ & SQL queries
        - Path 2: delete dataset & create dataset & upload data & load to GBQ & SQL queries
    """
    # Task 2 – Condition for delete / create dataset
    branch = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_task,
        provide_context=True,
        dag=dag2
    )

    """
    #### Task 2.1 – 'Deleting Dataset'
    If previous dataset was present in GBQ, it will be deleted
    """
    # Deleting Dataset
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id='delete_dataset',
        dataset_id=f'{PROJECT_ID}.dashboard_dataset',
        delete_contents=True,
        dag=dag2
    )

    """
    #### Task 2.2 – 'Creating Dataset'
    If previous dataset was absent in GBQ, it will be created
    """
    # Creating Dataset
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=bq_dataset
    )

    with TaskGroup("uploading_data_bucket") as uploading_data_bucket:
        """
        #### Task 3 – 'Uploading data to Bucket'
        Using LocalFilesystemToGCS Operators all cleaned data will be saved in Bucket folder - '/data'
        """

        # Upload Wine Table to Bucket
        upload_wine_db = LocalFilesystemToGCSOperator(
            task_id="upload_wine_bucket",
            src=os.path.join(base_path, "data/cleaned_data", "wines_db.csv"),
            dst=gcp_data_dest,
            bucket=gcp_bucket,
        )

        # Upload Wineries Table to Bucket
        upload_wineries_db = LocalFilesystemToGCSOperator(
            task_id="upload_wineries_bucket",
            src=os.path.join(base_path, "data/cleaned_data", "wineries_db.csv"),
            dst=gcp_data_dest,
            bucket=gcp_bucket,
        )

        # Upload Regions Table to Bucket
        upload_regions_db = LocalFilesystemToGCSOperator(
            task_id="upload_regions_bucket",
            src=os.path.join(base_path, "data/cleaned_data", "regions_db.csv"),
            dst=gcp_data_dest,
            bucket=gcp_bucket,
        )

        # Upload Harmonize Table to Bucket
        upload_harmonize_db = LocalFilesystemToGCSOperator(
            task_id="upload_harmonize_bucket",
            src=os.path.join(base_path, "data/cleaned_data", "harmonize_db.csv"),
            dst=gcp_data_dest,
            bucket=gcp_bucket,
        )

        # Upload Joined Table to Bucket
        upload_joined_db = LocalFilesystemToGCSOperator(
            task_id="upload_join-table_bucket",
            src=os.path.join(base_path, "data/cleaned_data", "joined_table_db.csv"),
            dst=gcp_data_dest,
            bucket=gcp_bucket,
        )

    with TaskGroup("loading_data_gbq") as loading_data_gbq:
        """
        #### Task 4 – 'Loading data to GBQ'
        Using GCSToBigQuery Operator all data from Bucket folder will be transferred to dataset - 'dashboard_dataset' 
        """

        # Load Wine Table to GBQ dataset
        load_gbq_wine = GCSToBigQueryOperator(
            task_id="load_to_bigquery_wine",
            bucket=gcp_bucket,
            source_objects=['data/wines_db.csv'],
            destination_project_dataset_table=f"{PROJECT_ID}.{bq_dataset}.wines_table",
            source_format="CSV",
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
            schema_fields=schema_wines
        )

        # Load Wineries Table to GBQ dataset
        load_gbq_wineries = GCSToBigQueryOperator(
            task_id="load_to_bigquery_wineries",
            bucket=gcp_bucket,
            source_objects=['data/wineries_db.csv'],
            destination_project_dataset_table=f"{PROJECT_ID}.{bq_dataset}.wineries_table",
            source_format="CSV",
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
            schema_fields=schema_wineries
        )

        # Load Regions Table to GBQ dataset
        load_gbq_regions = GCSToBigQueryOperator(
            task_id="load_to_bigquery_regions",
            bucket=gcp_bucket,
            source_objects=['data/regions_db.csv'],
            destination_project_dataset_table=f"{PROJECT_ID}.{bq_dataset}.regions_table",
            source_format="CSV",
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
            schema_fields=schema_regions
        )

        # Load Harmonize Table to GBQ dataset
        load_gbq_harmonize = GCSToBigQueryOperator(
            task_id="load_to_bigquery_harmonize",
            bucket=gcp_bucket,
            source_objects=['data/harmonize_db.csv'],
            destination_project_dataset_table=f"{PROJECT_ID}.{bq_dataset}.harmonize_table",
            source_format="CSV",
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
            schema_fields=schema_harmonize
        )

        # Load Joined Table to GBQ dataset
        load_gbq_join = GCSToBigQueryOperator(
            task_id="load_to_bigquery_join",
            bucket=gcp_bucket,
            source_objects=['data/joined_table_db.csv'],
            destination_project_dataset_table=f"{PROJECT_ID}.{bq_dataset}.joined_table",
            source_format="CSV",
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
            schema_fields=schema_joined_table
        )

    """
    #### Task 5 – 'Creating new Wine Clustered Table'
    Using BigQueryExecuteQuery Operator new Wine table clustered by Type & Country will be created
    """
    # Create new Wine clustered Table (Clustered by: Type, Country)
    create_cluster_table_wine = BigQueryExecuteQueryOperator(
        task_id="create_new_cluster_table_wine",
        sql="""
                CREATE TABLE IF NOT EXISTS `wine-de-dashboard.dashboard_dataset.wines_table_clustered` (
                    ABV FLOAT64 NOT NULL,
                    Acidity STRING NOT NULL,
                    Body STRING NOT NULL,
                    Country STRING NOT NULL,
                    Elaborate STRING NOT NULL,
                    Type STRING NOT NULL,
                    wine_url STRING, 
                    Vintages STRING,
                    WineID INT64 NOT NULL,
                    WineName STRING NOT NULL,
                    Avg_Rating FLOAT64 NOT NULL)
                    CLUSTER BY Type, Country;
                """,
        use_legacy_sql=False
    )

    """
    #### Task 6 – 'Inserting data to new Wine Clustered Table'
    Using BigQueryExecuteQuery Operator data from previous Wine table will be pulled in new clustered Wine Table
    """
    # Insert Data from previous Wine Table to Clustered Wine Table
    insert_data_cluster_wine = BigQueryExecuteQueryOperator(
        task_id="insert_data_into_cluster_table_wine",
        sql="""
            INSERT INTO `wine-de-dashboard.dashboard_dataset.wines_table_clustered` (ABV, Acidity, Body, Country, 
            Elaborate, Type, wine_url, Vintages, WineID, WineName, Avg_Rating)
            SELECT 
                CAST(ABV AS FLOAT64),
                Acidity, 
                Body, 
                Country, 
                Elaborate, 
                Type, 
                wine_url, 
                REPLACE(REPLACE(Vintages, ']', ''), '[', '') as Vintages, 
                WineID, 
                WineName, 
                ROUND(Avg_Rating,2) AS Avg_Rating
            FROM `wine-de-dashboard.dashboard_dataset.wines_table`
            """,
        use_legacy_sql=False
    )

    """
    #### Task 7 – 'Deleting previous Wine Table'
    Using BigQueryDeleteTable Operator previous Wine table will be deleted
    """
    # Delete previous Wine Table
    delete_previous_table_wine = BigQueryDeleteTableOperator(
        task_id="delete_previous_wine_table",
        deletion_dataset_table="wine-de-dashboard.dashboard_dataset.wines_table"
    )

# Execution sequence – Second DAG
# Create Branch Sequence
chain(check_dataset, branch)

# If dataset is present -> delete & create & upload & load & create_cluster_table
chain(delete_dataset, create_dataset, uploading_data_bucket, loading_data_gbq, create_cluster_table_wine,
      insert_data_cluster_wine, delete_previous_table_wine)
# If dataset is absent -> create & upload & load & create_cluster_table
chain(create_dataset, uploading_data_bucket, loading_data_gbq,
      create_cluster_table_wine, insert_data_cluster_wine, delete_previous_table_wine)
