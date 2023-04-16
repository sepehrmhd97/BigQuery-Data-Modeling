# Data Warehousing in BigQuery

This repository contains an example project that demonstrates how to create a data warehouse with dimensional modeling in BigQuery using its API in Python. This project showcases an ETL processing with four functions that extracts data from source files, loads it into a staging area, cleans abd transforms the data, creates the warehouse schema, and loads the data into the warehouse. Different information such as the schema of the data warehouse, the data sources, and the data transformations are stored in a JSON file. This allows the ETL process to be easily modified and extended to include new data sources and transformations.


### What is a Data Warehousing?

Data warehousing is a process that involves collecting data from multiple sources, transforming it into a format that is optimized for analysis, and loading it into a data warehouse. Data warehouses are optimized for querying and reporting, and are often used to create dashboards and reports.

![Alt text](/Pictures/datawarehouse.jpg)

**Staging Area**
A staging area is a temporary storage location where data is copied before it is loaded into a data warehouse. The purpose of a staging area is to allow data to be cleaned, transformed, and validated before it is loaded into the warehouse.

**Data Mart**
A data mart is a subset of a data warehouse that contains a specific set of data that is relevant to a particular business function or department. Data marts are often used to create reports and dashboards for a specific department or business function.

### What is dimensional data modeling?

Dimensional data modeling is a design technique used to organize data into a structure that is optimized for querying and reporting. It involves organizing data into "facts" (numerical data that can be aggregated) and "dimensions" (descriptive data that can be used to slice and filter the facts), and creating relationships between them. It is a common approach to data modeling in data warehouses.

![Alt text](/Pictures/star-schema.png)

### Pre-requisites
To work with the code in this repository, you will need a Google Cloud Platform account and Python 3.6 or later installed on your machine.

- install BigQuery: `pip install google-cloud-bigquery`
- Create a service account and download the JSON key file from the Google Cloud Console. You can follow the instructions in the [BigQuery documentation](https://developers.google.com/workspace/guides/create-credentials) to do this.

## Getting Started

``` python
import os
from google.cloud import bigquery
import pandas as pd
import json

# Set the environment variable GOOGLE_APPLICATION_CREDENTIALS to the path of the JSON key file that you downloaded earlier.
credentials = os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="path/to/your/credentials.json"
```

### 1. Loading the data into BigQuery - Staging Area

**From CSV source** :This Python function can be used to upload a CSV file to BigQuery. The function takes the path to the CSV file, the project ID, and the table name as arguments, and uploads the file to a dataset named 'staging' in BigQuery based on the schema defined in the JSON file

``` python
#wxtracting data from csv file and loading it into staging area with the desired schema
def load_csv_to_bigquery(csv_path, project_id, table_name, schema_file_path):
    dataset_name = 'staging'
    # Create a BigQuery client using your service account key file
    client = bigquery.Client(project=project_id)

    # Read the CSV file into a Pandas dataframe
    df = pd.read_csv(csv_path, encoding='ISO-8859-1')

    # Replace spaces and dashes with underscores in column names
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')

    # Create the BigQuery dataset if it doesn't exist
    dataset_ref = client.dataset(dataset_name)
    try:
        client.get_dataset(dataset_ref)
        print("Dataset {} already exists".format(dataset_name))
    except:
        print("Creating dataset {}".format(dataset_name))
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset)

    # Set the destination table for the data
    table_ref = dataset_ref.table(table_name)

    # Define the schema of the table
    with open(schema_file_path) as schema_file:
        schema_json = json.load(schema_file)

    # Replace spaces and hyphens with underscores in field names
    for field in schema_json:
        field['name'] = field['name'].replace(' ', '_').replace('-', '_')

    # Create the schema field objects
    schema = [bigquery.SchemaField.from_api_repr(field) for field in schema_json]

    # Create the table in BigQuery
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)

    # Load the data into the table
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = False # Set to True to automatically detect schema, False to use schema defined above
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    print("Data loaded to Staging Area successfully.")



```

**From xslx source** :This Python function can be used to upload an xslx file to BigQuery. The function takes the path to the xslx file, the project ID, and the table name as arguments, and uploads the file to a dataset named 'staging' in BigQuery based on the schema defined in the JSON file

``` python

def load_xlsx_to_bigquery(xlsx_path, project_id, table_name, schema_file_path):
    dataset_name = 'staging'
    # Create a BigQuery client using your service account key file
    #credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project_id)

    # Read the xlsx file into a Pandas dataframe
    df = pd.read_excel(xlsx_path, engine='openpyxl')

    # Remove time part from datetime columns
    for column in df.columns:
        if df[column].dtype == 'datetime64[ns]':
            df[column] = df[column].dt.date

    # Replace spaces with underscores in column names
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')

    # Create the BigQuery dataset if it doesn't exist
    dataset_ref = client.dataset(dataset_name)
    try:
        client.get_dataset(dataset_ref)
        print("Dataset {} already exists".format(dataset_name))
    except:
        print("Creating dataset {}".format(dataset_name))
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset)

    # Set the destination table for the data
    table_ref = dataset_ref.table(table_name)

    # Define the schema of the table
    with open(schema_file_path) as schema_file:
        schema_json = json.load(schema_file)

    # Replace spaces with underscores in schema field names
    for field in schema_json:
        field['name'] = field['name'].replace(' ', '_')

    # Create the schema field objects
    schema = [bigquery.SchemaField.from_api_repr(field) for field in schema_json]

    # Create the table in BigQuery
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)

    # Load the data into the table
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = False # Set to True to automatically detect schema, False to use schema defined above
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    print("Data uploaded to BigQuery successfully.")
```

#### BigQuery Schema autodetect, yes or no?
In terms of computational power, it's generally more efficient to provide the schema yourself rather than relying on BigQuery to auto-detect it. This is because auto-detecting the schema requires BigQuery to scan the entire file to determine the data types of each column, which can be time-consuming and resource-intensive for large files.

Providing the schema upfront using Pandas allows you to bypass this schema detection step entirely and can result in faster load times. Additionally, providing the schema yourself also ensures that the data types are correctly inferred, as auto-detection may sometimes fail to identify the correct data type for a column.

That being said, auto-detection can be useful in situations where you don't know the schema of your data in advance, or if the schema is likely to change over time. In these cases, auto-detection can save you the effort of manually updating the schema each time it changes.

The following code snippet shows how to upload data to BigQuery using auto-detection by setting autodetect to True:

``` python
# Load data from local machine to BigQuery - staging area
def load_csv_to_bigquery(csv_path, project_id, table_name):
    dataset_name = 'staging'
    # Create a BigQuery client 
    client = bigquery.Client(project=project_id)

    # Create the BigQuery dataset if it doesn't exist
    dataset_ref = client.dataset(dataset_name)
    try:
        client.get_dataset(dataset_ref)
        print("Dataset {} already exists".format(dataset_name))
    except:
        print("Creating dataset {}".format(dataset_name))
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset)

    # Set the destination table for the data
    table_ref = dataset_ref.table(table_name)

    # Create the table in BigQuery with schema autodetection
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True # Set to True to automatically detect schema
    job = client.load_table_from_uri(
        csv_path,
        table_ref,
        job_config=job_config
    )
    job.result()  # Wait for the job to complete.

    print("Data uploaded to BigQuery successfully.")
```

### 2. Data Transformation

**Step 1:** This Python function is an example of a data transform and loading function that can be used as part of an ETL pipeline. The function takes the project ID and table ID as input, with optional arguments for cleaning data  The function then perform below tasks on the the data, and uploads it to a new table in BigQuery.

- Remove null values for columns to check that are primary keys of dimension tables
- Remove duplicate rows for columns to check that are primary keys of dimension tables
- Convert date columns to date format
- Round float columns to a specified number of decimal places

``` python
def data_transform(project_id, table_id, remove_nulls=False, remove_duplicates=False, date_columns=None, columns_to_check=None):
    """
    Clean a BigQuery table by removing null values and/or duplicates, and rounding float columns.

    Args:
        project_id (str): The Google Cloud Project ID.
        table_id (str): The BigQuery table ID.
        remove_nulls (bool, optional): Whether to remove rows with null values. Defaults to False.
        columns_to_check (list, optional): List of columns to check for null values or duplicates. Defaults to None (all columns).
        remove_duplicates (bool, optional): Whether to remove duplicate rows. Defaults to False.
        date_columns (list, optional): List of columns to convert to date format. Defaults to None.
        float_rounding_decimal (int, optional): Number of decimal places to round float columns. Defaults to 2.

    Returns:
        None
    """
    client = bigquery.Client()
    table_ref = client.get_table(table_id)
    table = client.get_table(table_ref)

    if columns_to_check is None:
        columns_to_check = [field.name for field in table.schema]

    sql_base = f"SELECT * FROM `{table_id}`"
    sql_conditions = []

    #remove nulls for selected columns in columns to check
    if remove_nulls:
        not_null_conditions = [f"{column} IS NOT NULL" for column in columns_to_check]
        sql_conditions.append(" AND ".join(not_null_conditions))

    #removes duplicates based on columns to check which are the primary keys of dimensions
    if remove_duplicates:
        row_number_clause = f", ROW_NUMBER() OVER (PARTITION BY {', '.join(columns_to_check)} ORDER BY {', '.join([field.name for field in table.schema])}) AS row_number"
        deduplicate_condition = "row_number = 1"
    else:
        row_number_clause = ""
        deduplicate_condition = "TRUE"

    if sql_conditions:
        sql_condition = "WHERE " + " AND ".join(sql_conditions)
    else:
        sql_condition = ""

    # Handle date column transformation, float column rounding, and make all columns lower case
    select_columns = []
    if date_columns is None:
        date_columns = []
    #round the float values to 2 decimal places, extract year, mont, and day for dates columns
    for column in table.schema:
        if column.field_type == "FLOAT":
            select_columns.append(f"ROUND({column.name}, {2}) AS {column.name.lower()}")
        elif column.name in date_columns:
            select_columns.append(f"CAST({column.name} AS DATE) AS {column.name.lower()}")
            select_columns.append(f"EXTRACT(YEAR FROM CAST({column.name} AS DATE)) AS {column.name.lower()}_year")
            select_columns.append(f"EXTRACT(MONTH FROM CAST({column.name} AS DATE)) AS {column.name.lower()}_month")
            select_columns.append(f"EXTRACT(DAY FROM CAST({column.name} AS DATE)) AS {column.name.lower()}_day")
        else:
            select_columns.append(column.name.lower())

    sql = f"SELECT {', '.join(select_columns)} FROM (SELECT {', '.join(select_columns)}{row_number_clause} FROM ({sql_base}) AS subquery {sql_condition}) AS subquery_with_row_number WHERE {deduplicate_condition}"

    # Execute the query and save the results to a new table
    new_table_id = f"{project_id}.{table_ref.dataset_id}.{table_ref.table_id}_cleaned"
    new_table_ref = client.dataset(table_ref.dataset_id).table(f"{table_ref.table_id}_cleaned")

    job_config = bigquery.QueryJobConfig(destination=new_table_ref)
    query_job = client.query(sql, job_config=job_config)
   

    query_job.result()

    print(f"Cleaned table saved as {new_table_id}.")


```
Whether to keep the original table or replace it with the cleaned one depends on your specific use case and requirements. Here are some factors to consider when making this decision:

- *Data history and traceability:* If you need to maintain a record of the original data for auditing, traceability, or historical analysis purposes, it is better to keep the original table and create a separate cleaned table. This way, you can always refer back to the original data if needed.

- *Data storage costs:* Storing multiple versions of the same table may increase your storage costs in BigQuery. If storage costs are a concern and you're confident that you won't need to access the original data again, you can consider replacing the original table with the cleaned one.

**Step 2:** Below function adds Surrogae key for all the considered tables in the data warehouse. The surrogate key is a unique identifier for each row in a table. It is used to link the fact table to the dimension tables. The surrogate key is usually an auto-incrementing integer. In this case, we are using the `ROW_NUMBER()` query function to generate the surrogate key. 

This python function takes a JSON file as input which contains the following information:

- `surrogate_key_column`: The name of the surrogate key column.
- `base_columns`: The list of columns that will be used to generate the surrogate key.
- `starting_point`: The starting point for the surrogate key. This is necessory because we may already have data from initial or previous loads in the warehouse that we dont want the surrogate keys to conflict.

``` python
def generate_surrogate_keys(table_id, json_file):
    with open(json_file, 'r') as f:
        surrogate_keys_data = json.load(f)

    for data in surrogate_keys_data:
        surrogate_key_column = data["surrogate_key_column"]
        base_columns = data["base_columns"]
        starting_point = data["starting_point"]

        generate_single_surrogate_key(table_id, surrogate_key_column, base_columns, starting_point)

def generate_single_surrogate_key(table_id, surrogate_key_column, base_columns, starting_point):
    client = bigquery.Client()

    # Prepare the base columns for use in the query
    base_columns_str = ", ".join(base_columns)

    temp_table_id = f"{table_id}_temp"

    # Create a temporary table with unique combinations of base columns and surrogate keys
    create_temp_table_query = f"""
        CREATE OR REPLACE TABLE `{temp_table_id}` AS
        SELECT
            {base_columns_str},
            ROW_NUMBER() OVER () + {starting_point} - 1 AS {surrogate_key_column}
        FROM
            (SELECT DISTINCT {base_columns_str} FROM `{table_id}`)
    """

    # Update the original table with the surrogate keys from the temporary table
    update_original_table_query = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS
        SELECT
            A.*,
            B.{surrogate_key_column}
        FROM
            `{table_id}` A
        JOIN
            `{temp_table_id}` B
        ON
            { " AND ".join([f"A.{col} = B.{col}" for col in base_columns]) }
    """

    # Drop the temporary table
    drop_temp_table_query = f"""
        DROP TABLE `{temp_table_id}`
    """

    # Execute the queries
    client.query(create_temp_table_query).result()
    client.query(update_original_table_query).result()
    client.query(drop_temp_table_query).result()

    print(f"Surrogate key '{surrogate_key_column}' generated for table '{table_id}' using columns '{base_columns_str}'.")



``` 

##### Handling Slowly Changing Dimensions (SCD):
Slowly Changing Dimensions (SCD) are a common challenge in data warehousing, where dimensions of the data change over time. These changes can be new records, updates to existing records, or changes in the relationships between records. There are different types of SCDs, such as Type 1, Type 2, and Type 3, each representing various methods of handling these changes.
![Alt text](/Pictures/scd.png)

Using surrogate keys is an effective way to manage SCDs. A surrogate key is a unique identifier, typically an integer, assigned to each record in a dimension table, independent of the natural or business keys. This ensures that the record can be uniquely identified even when the natural key or other attributes change. For instance, in an SCD Type 2 scenario, a new record with a new surrogate key is added to the dimension table when a change occurs, preserving the history of the original record while still maintaining a unique identifier for the new version of the record. By using surrogate keys, the data warehouse can effectively track the history of changes and maintain referential integrity, enabling accurate reporting and analysis over time.
### 3. Create the Warehouse Schema

The warehouse schema is the final schema that will be used for analysis and reporting. It is the schema that will be used by the end users to query the data. It is also usually denormalized, meaning that it contains data from multiple tables in the staging schema.

The `create_warehouse_schema`  function creates a data warehouse schema in Google BigQuery based on the input provided in a JSON file. The schema consists of a fact table, dimension tables, and mapping tables to link them together.

The function takes as inputs the GCP project ID and the path to the JSON file containing the schema information. The fact and dimension tables are created with their corresponding columns, and the mapping tables are created to link the fact table to each dimension table based on the specified keys.

Overall, this function provides a simple and efficient way to set up a data warehouse schema for dimensional data modeling using the Google BigQuery platform.

``` python

#creating a warehouse schema from json file
def create_warehouse_schema(project_id, json_path):
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Load the schema information from the JSON file
    with open(json_path, 'r') as f:
        schema_info = json.load(f)

    # Create a dataset named "warehouse" (if it doesn't already exist)
    dataset_id = "warehouse"
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
    except:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)

    # Define fact table schema
    fact_table_name = schema_info['fact_table_name']
    fact_table_columns = [bigquery.SchemaField(field['name'], field['type'], mode=field.get('mode', 'NULLABLE')) 
                          for field in schema_info['fact_table_columns']]
    fact_table_ref = client.dataset(dataset_id).table(fact_table_name)
    fact_table = bigquery.Table(fact_table_ref, schema=fact_table_columns)
    fact_table = client.create_table(fact_table)  # API request

    # Create dimension tables
    for dimension_table_name, dimension_table_info in schema_info['dimension_tables'].items():
        dimension_table_columns = [bigquery.SchemaField(field['name'], field['type'], mode=field.get('mode', 'NULLABLE'))
                                   for field in dimension_table_info]
        dimension_table_ref = client.dataset(dataset_id).table(dimension_table_name)
        dimension_table = bigquery.Table(dimension_table_ref, schema=dimension_table_columns)
        dimension_table = client.create_table(dimension_table)  # API request

    # Create fact-dimension mapping tables
    for fact_column, dimension_map in schema_info['fact_dimension_key_map'].items():
        for dimension_column, dimension_table_name in dimension_map.items():
            mapping_table_name = f"{fact_table_name}_{dimension_table_name}"
            mapping_table_columns = [
                bigquery.SchemaField(fact_column, 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField(dimension_column, 'INTEGER', mode='REQUIRED'),
            ]
            mapping_table_ref = client.dataset(dataset_id).table(mapping_table_name)
            mapping_table = bigquery.Table(mapping_table_ref, schema=mapping_table_columns)
            mapping_table = client.create_table(mapping_table)  # API request

    print("Warehouse schema created successfully.")

    
```

### 4. Load the Data into the Warehouse

This Python script provides a function to transfer data from a staging table to multiple warehouse tables in BigQuery. The function is designed to copy specific columns from the staging table to each warehouse table based on the schema of the warehouse tables.

``` python
#loading data from staging to warehouse
def load_data_from_staging_to_warehouse(project_id, dataset_warehouse, dataset_staging, staging_table_id, warehouse_table_names):
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Iterate through the warehouse table names
    for warehouse_table_name in warehouse_table_names:
        # Get the schema of the warehouse table
        warehouse_table = client.get_table(f"{project_id}.{dataset_warehouse}.{warehouse_table_name}")
        warehouse_columns = [field.name for field in warehouse_table.schema]

        # Create a query to select and cast specific columns from the staging table
        source_columns = ', '.join([f"CAST({field.name} AS {field.field_type.replace('FLOAT', 'FLOAT64')}) AS {field.name}" for field in warehouse_table.schema])
        sql = f"""
            SELECT {source_columns}
            FROM `{project_id}.{dataset_staging}.{staging_table_id}`
        """

        # Create table references
        destination_table_ref = f"{project_id}.{dataset_warehouse}.{warehouse_table_name}"

        # Create a load job configuration
        job_config = bigquery.QueryJobConfig()
        job_config.destination = destination_table_ref
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        # Run the query job
        query_job = client.query(sql, job_config=job_config)
        query_job.result()

        print(f"Data copied from {dataset_staging}.{staging_table_id} to {dataset_warehouse}.{warehouse_table_name}")
```

## Usage

By running the below code snippet, you can create a data warehouse in BigQuery based on the schema information provided in the JSON file for the superstore dataset.

``` python
#loading data
csv_path = "path to csv file"
project_id = "your project id"
table_name = "superstore"
load_csv_to_bigquery(csv_path = csv_path, project_id = project_id, table_name = table_name)
#clean the table
table_id = "your_project_id.staging.superstore"
date_columns=["Order_Date", "Ship_Date"]
columns_to_check=["Customer_ID", "Order_Date", "Order_ID", "Product_ID"]
clean_bigquery_table(project_id = project_id, table_id = table_id, remove_nulls=True, remove_duplicates=True, date_columns=date_columns, columns_to_check=columns_to_check)

#creating warehouse schema from json file
json_path = "path to json file"
create_warehouse_schema(project_id = project_id, json_path = json_path)

#loading data from staging to warehouse
dataset_warehouse = "warehouse"
dataset_staging = "staging"
staging_table_id = "superstore_cleaned"
warehouse_table_names = ["date_dim", "customer_dim", "product_dim", "sales_fact","sales_fact_customer_dim_customer_id", 
                         "sales_fact_date_dim_order_date", "sales_fact_product_dim_product_id"]
load_data_from_staging_to_warehouse(project_id, dataset_warehouse, dataset_staging, staging_table_id, warehouse_table_names)

```










