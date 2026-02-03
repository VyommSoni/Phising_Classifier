import os
import sys
import pymongo
from src.exception import CustomException
from src.constants import *
import certifi
ca=certifi.where()

class MongoClient:
    client=None

    def __init__(self,database_name=MONGO_DATABASE_NAME):
        try:

         if MongoClient.client is None:
            mongo_ab_url=os.getenv("MONGO_DB_URL")
            if mongo_ab_url is None:
                raise Exception('mongo ab url is none..')
            MongoClient.client=pymongo.MongoClient(mongo_ab_url,tlsCAFILE=ca)
         self.client=MongoClient.client
         self.database=self.client[database_name]
         self.database_name=database_name
        except Exception as e:
           raise CustomException(e,sys)


