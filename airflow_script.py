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
import pickle
from io import StringIO
from smart_open import smart_open

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

# [END import_module]

"""con = snowflake.connector.connect(
    user='KOBLE9493', # Change it with actual value of your snowflake.
    password='Katelyn1', # Change it with actual value of your snowflake.
    account='OCROCTL-IBB37511', # Change it with actual value of your account.
    warehouse="COMPUTE_WH", # Change it with actual value of your snowflake.
    database="CHURNAI101", # Change it with actual value of your snowflake.
    schema="PUBLIC" # Change it with actual value of your snowflake.
)"""


AWS_ACCESS_KEY_ID = 'AKIAXYKJQIA56PQWW5XI' # Change it with actual value of your AWS account.
AWS_SECRET_ACCESS_KEY = '/TiyelIE/Ln1dWqIhIqtj8iQ/8zFdSWedAjU1TEj' # Change it with actual value of your AWS account.

# [START instantiate_dag]
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
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
        """cur = con.cursor()
        sql = 'select * from "CHURN API"'
        cur.execute(spl)

        sf_table = cur.fetch_pandas_all()
        """
        sf_table = pd.read_csv('Train_Churn.csv')

        return sf_table

    # [END extract]

    # [START transform]
    @task()
    def transform_model_build(sf_table):
        
        df = sf_table
        df['GENDER'] = df['GENDER'].apply(lambda x: 1 if (x == 'Male' or x ==  'male') else 0)

        df['GEOGRAPHY'] = df['GEOGRAPHY'].apply(lambda x: 0 if x == 'France' else (1 if x == 'Germany' else 2))

        df = df.drop(['CUSTOMERID', 'SURNAME'], axis = 1)

        feature_set = ['CreditScore', 'Geography','Gender','Age','Tenure', 'Balance', 'NumOfProducts', 'HasCrCard', 'IsActiveMember','EstimatedSalary']
        X = df[feature_set]
        y = df['Exited']

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=0)

        rm = RandomForestClassifier(n_estimators = 10, max_depth=25, criterion = "gini", min_samples_split=10)
        rm.fit(X_train, y_train)

        return rm






       

    # [END transform]

    # [START load]
    @task()
    def load(model):
        """
        #### Load task
        Load the output pickle file to S3 bucket.
        """
        # Save the model to a pickle file
        with open('Churn_model.pkl', 'wb') as file:
            pickle.dump(model, file)

        # print("Model saved to trained_model_RF.pkl")

       # session = boto3.Session(
       # aws_access_key_id=AWS_ACCESS_KEY_ID,
       #     aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
       # )
        s3 = session.resource('s3')

        BUCKET = "ai101"
        key = "Katelyn_Churn/Churn_model.pkl"

        pickle_byte_obj = pickle.dumps(model)
        s3.Object(BUCKET,key).put(Body=pickle_byte_obj)


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