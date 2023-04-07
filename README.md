# Dimensional Data Modeling in BigQuery

This repository contains an example project that demonstrates how to create a dimensional data model with a star schema in BigQuery using the Python API. 

### What is dimensional data modeling?

Dimensional data modeling is a design technique used to organize data into a structure that is optimized for querying and reporting. It involves organizing data into "facts" (numerical data that can be aggregated) and "dimensions" (descriptive data that can be used to slice and filter the facts), and creating relationships between them. It is a common approach to data modeling in data warehouses.

![Alt text](/Pictures/star-schema.png)

### What is a Data Warehousing?

Data warehousing is a process that involves collecting data from multiple sources, transforming it into a format that is optimized for analysis, and loading it into a data warehouse. Data warehouses are optimized for querying and reporting, and are often used to create dashboards and reports.

![Alt text](/Pictures/datawarehouse.jpg)

**Staging Area**
A staging area is a temporary storage location where data is copied before it is loaded into a data warehouse. The purpose of a staging area is to allow data to be cleaned, transformed, and validated before it is loaded into the warehouse.

**Data Mart**
A data mart is a subset of a data warehouse that contains a specific set of data that is relevant to a particular business function or department. Data marts are often used to create reports and dashboards for a specific department or business function.

### Pre-requisites
To work with the code in this repository, you will need a Google Cloud Platform account with billing enabled, access to the Google Analytics sample dataset in BigQuery, and Python 3.6 or later installed on your machine.

- install BigQuery: `pip install google-cloud-bigquery`
- Create a service account and download the JSON key file from the Google Cloud Console. You can follow the instructions in the [BigQuery documentation](https://developers.google.com/workspace/guides/create-credentials) to do this.

##Getting Started

``` python
import os
from google.cloud import bigquery
import pandas as pd

# Set the environment variable GOOGLE_APPLICATION_CREDENTIALS to the path of the JSON key file that you downloaded earlier.
credentials = os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="path/to/your/credentials.json"
```

#### 1. Loading the data into BigQuery - Staging Area

This Python function can be used to upload a CSV file to BigQuery. The function takes the path to the CSV file, the project ID, and the table name as arguments, and uploads the file to a dataset named 'staging' in BigQuery.

``` python
#load data from local machine to bigquery - staging area
def load_csv_to_bigquery(csv_path, project_id, table_name):
    dataset_name = 'staging'
    # Create a BigQuery client 
    client = bigquery.Client(project=project_id)

    # Read the CSV file into a Pandas dataframe
    df = pd.read_csv(csv_path)

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
    schema = []
    for column in df.columns:
        schema.append(bigquery.SchemaField(column, 'STRING'))

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

##### BigQuery Schema autodetect, yes or no?
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










