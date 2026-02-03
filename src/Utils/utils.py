import os
import pickle
import pandas as pd
import numpy as np
import boto3
import yaml
import sys
from src.exception import CustomException
from src.logger import logging
from src.constants import *

class MainUtils:
    def __init__(self)->None:
        pass
    
    def read_yamlfile(self,filename:str)->dict:
        try:
            with open(filename,'r') as yaml_file:
                return yaml.safe_load(yaml_file)
        
        except Exception as e:
            raise CustomException(e,sys) from e
        
    def read_Schema_Config(self)->dict:
        try:
            os.makedirs('Config',exist_ok=True)
            Schema_config=self.read_yamlfile(os.path.join('Config','model.yaml'))
            return Schema_config
        except Exception as e:
            raise CustomException(e,sys) from e
        
    @staticmethod
    def save_object(file_path:str,obj:object)->None:
        logging.info('Enter into save object fuction...!')
        try:
            with open(file_path,'wb') as file:
                pickle.dump(obj,file)
            logging.info('Exiting the fucntion after saving the object')

        except Exception as e:
         raise CustomException (e,sys) from e
        
    @staticmethod
    def load_object(filepath:str)->None:
        logging.info('Entering into load_object function')
        try:
            with open(filepath ,'rb') as file:
                obj=pickle.load(filepath)

            logging.info('Exiting the load object function')
        except Exception as e:
            raise CustomException(e,sys) from e
    
    @staticmethod
    def upload_files(from_file,to_file,bucket_name):
        try:
         s3_resource=boto3.resource("s3")
         s3_resource.meta.client.upload_file(from_file,bucket_name,to_file)

        except Exception as e :
            raise CustomException(e,sys) from e
    @staticmethod
    def download_object(bucket_name,bucket_filename,dest_file):
        try:
            s3_client=boto3.client("s3")
            s3_client.download_file(bucket_name,bucket_filename,dest_file)
            return dest_file
        except Exception as e:
            raise CustomException(e,sys) from e
    
    @staticmethod
    def remove_unwanted_space(Data:pd.DataFrame)->pd.DataFrame:
        try:
            data_without_spaces=Data.apply(lambda x:x.str.strip() if x.dtype=='object' else x)
            logging.info('Unwanted space removed,Now exiting the funtion..')
            return data_without_spaces
        
        except Exception as e :
            raise CustomException(e,sys) from e
    
        











        

