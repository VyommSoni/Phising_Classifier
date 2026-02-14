import os
import sys
import pymongo
from src.exception import CustomException
from src.constants import *
import certifi
ca=certifi.where()
from dotenv import load_dotenv
load_dotenv()

class MongoDBClient:
    client=None

    def __init__(self,database_name=MONGO_DATABASE_NAME):
        try:

         if MongoDBClient.client is None:
            mongo_db_url=os.getenv("MONGO_DB_URL")
            if mongo_db_url is None:
                raise Exception('mongo ab url is none..')
            MongoDBClient.client=pymongo.MongoClient(mongo_db_url,tlsCAFILE=ca)
         self.client=MongoDBClient.client
         self.database=self.client[database_name]
         self.database_name=database_name
        except Exception as e:
           raise CustomException(e,sys) from e


