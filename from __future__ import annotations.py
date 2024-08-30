from __future__ import annotations

# [START tutorial]
# [START import_module]
import json
import pendulum

from airflow.decorators import dag, task
import pandas as pd
import snowflake.connector
import boto3
import numpy as np
from collections import Counter
from sklearn.metrics import accuracy_score, precision_score, recall_score
from sklearn.model_selection import train_test_split
import pickle
from io import StringIO
from smart_open import smart_open
from sklearn.ensemble import RandomForestClassifier

# [END import_module]

"""con = snowflake.connector.connect(
    user='koble9493', # Change it with actual value of your snowflake.
    password='Katelyn1', # Change it with actual value of your snowflake.
    account='OCROCTL-IBB37511', # Change it with actual value of your account.
    warehouse="COMPUTE_WH", # Change it with actual value of your snowflake.
    database="CHURNAI101", # Change it with actual value of your snowflake.
    schema="PUBLIC" # Change it with actual value of your snowflake.
)"""


AWS_ACCESS_KEY_ID = 'xxxx' # Change it with actual value of your AWS account.
AWS_SECRET_ACCESS_KEY = 'xxxx' # Change it with actual value of your AWS account.

# [START instantiate_dag]
@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 8, 26, tz="EST"),
    catchup=False,
    tags=["example"],
)
def ai101_pipeline():

    # [START extract]
    @task()
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline.
        """
        
        cur = con.cursor()
        sql = "select * from 

        return sf_table

    # [END extract]

    # [START transform]
    @task()
    def transform_model_build(sf_table):
        
        

    # [END transform]

    # [START load]
    @task()
    def load(model):
        """
        #### Load task
        Load the output pickle file to S3 bucket.
        """
        # Save the model to a pickle file
        with open('trained_model_RForest2.pkl', 'wb') as file:
            pickle.dump(model, file)

        # print("Model saved to trained_model_RF.pkl")

        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        s3 = session.resource('s3')

        BUCKET = "ai101"

        s3.Bucket(BUCKET).upload_file("./trained_model_RForest2.pkl", "piyush/trained_model_RForest2.pkl")

    # [END load]

    # [START main_flow]
    sf_table = extract()
    model = transform_model_build(sf_table)
    load(model)
    # [END main_flow]


# [START dag_invocation]
ai101_pipeline()
# [END dag_invocation]

# [END tutorial]