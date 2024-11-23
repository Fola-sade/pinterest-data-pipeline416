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
            
            #print(pin_result)
            #print(geo_result)
            #print(user_result)
        

            pin_payload = json.dumps({
                "StreamName": "streaming-0affe4008223-pin",
                "Data": {
                    "index": pin_result["index"],
                    "unique_id": pin_result["unique_id"],
                    "title": pin_result["title"],
                    "description": pin_result["description"],
                    "poster_name": pin_result["poster_name"],
                    "follower_count": pin_result["follower_count"],
                    "tag_list": pin_result["tag_list"],
                    "is_image_or_video": pin_result["is_image_or_video"],
                    "image_src": pin_result["image_src"],
                    "downloaded": pin_result["downloaded"],
                    "save_location": pin_result["save_location"],
                    "category": pin_result["category"]
                    },
                    "PartitionKey": "pin_pk"
                    })
        
            headers = {'Content-Type': 'application/json'}
            # invoke_url = "https://YourAPIInvokeURL/<YourDeploymentStage>/streams/<stream_name>/record" (invole_url found from deploying the API from API Gateway)
            invoke_url_pin = "https://9kqs9brm97.execute-api.us-east-1.amazonaws.com/prod/streams/streaming-0affe4008223-pin/record"
            pin_response = requests.request("PUT", invoke_url_pin, headers=headers, data=pin_payload)
            print("**************************************************\n", "[PIN]\n", "Status code: ", pin_response.status_code, "\n\n", "pin_response.json(): ", pin_response.json(), "\n\n", "pin_payload: ", pin_payload, "\n\n")

            geo_payload = json.dumps({
                "StreamName": "streaming-0affe4008223-geo",
                "Data": {
                    "ind": geo_result["ind"],
                    "timestamp": geo_result["timestamp"].strftime('%Y-%m-%d %H:%M:%S'),
                    "latitude": geo_result["latitude"],
                    "longitude": geo_result["longitude"],
                    "country": geo_result["country"]
                    },
                    "PartitionKey": "geo_pk"
                    })
            
            invoke_url_geo = "https://9kqs9brm97.execute-api.us-east-1.amazonaws.com/prod/streams/streaming-0affe4008223-geo/record"
            geo_response = requests.request("PUT", invoke_url_geo, headers=headers, data=geo_payload)
            print("[GEO]\n", "Status code: ", geo_response.status_code, "\n\n", "geo_response.json(): ", geo_response.json(), "\n\n", "geo_payload: ", geo_payload, "\n\n")
            
            user_payload = json.dumps({
                 "StreamName": "streaming-0affe4008223-user",
                "Data": {
                    "ind": user_result["ind"],
                    "first_name": user_result["first_name"],
                    "last_name": user_result["last_name"],
                    "age": user_result["age"],
                    "date_joined": user_result["date_joined"].strftime("%Y-%m-%d %H:%M:%S")
                    },
                    "PartitionKey": "user_pk"
                    })
            
            invoke_url_user = "https://9kqs9brm97.execute-api.us-east-1.amazonaws.com/prod/streams/streaming-0affe4008223-user/record"
            user_response = requests.request("PUT", invoke_url_user, headers=headers, data=user_payload)
            print("[USER]\n", "Status code: ", user_response.status_code, "\n\n", "user_response.json(): ", user_response.json(), "\n\n", "user_payload: ", user_payload, "\n\n")

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
