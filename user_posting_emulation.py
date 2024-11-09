import requests
import sys
sys.stdout.reconfigure(encoding='utf-8')
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
import yaml
from sqlalchemy import text


random.seed(100)


class AWSDBConnector:

    def __init__(self):
        self.creds={}

    def read_db_creds(self):
        """
        Returns the database credentials from the yaml file
        """
        with open('creds.yaml') as yaml_file:
            self.creds = yaml.safe_load(yaml_file)
        return self.creds

        
    def create_db_connector(self):
        config=self.read_db_creds()
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{config["USER"]}:{config["PASSWORD"]}@{config["HOST"]}:{config["PORT"]}/{config["DATABASE"]}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)  
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print(pin_result)
            print(geo_result)
            print(user_result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


