import os
import sys
import shutil 
import re
from pathlib import Path
from typing import List
import pandas as pd
import numpy as np
import json
from src.constants import *
from dataclasses import dataclass
from  src.exception import CustomException
from src.Utils import utils
from src.logger import logging

Length_of_timestamp_in_file=6
Length_of_Datestamp_in_file=8
No_of_columns=11

@dataclass
class DataValidationConfig:
    data_validation_dir:str=os.path.join('artifacts','data_validation')
    valid_data_dir:str=os.path.join(data_validation_dir,'validated')
    invalid_data_dir:str=os.path.join(data_validation_dir,'invalid_data')
    schema_config_file_path:str=os.path.join('config','training_schema.json')

class DataValidation:
    def __init__(self,raw_data_store_dir:str):
        self.raw_data_dir=raw_data_store_dir
        self.data_validation_config=DataValidationConfig()
        self.utils=utils()

    def valuesfromschema(self):
        try:
            with open(self.data_validation_config.schema_config_file_path,'r') as f :
              dic=json.load(f)
              f.close()
            LengthOfDateStampsInFile=dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile=dic['LengthOfTimeStampInFile']
            Col_names=dic['ColName']
            Number_of_Columns=dic['NumberofColumns']

            return LengthOfDateStampsInFile,LengthOfTimeStampInFile,Col_names,Number_of_Columns
        except Exception as e:
            raise CustomException(e,sys) from e
        
    def validatefilename(self,filepath:str,length_of_timestamp:int,
                         length_of_datestamp:int)->bool :
        try:
            filename=os.path.basename(filepath)
            original_files='phising_copy.csv'

            if re.match(original_files,filename):
                SplitAtDot=re.split('.csv',filename)
                SplitAtDot=(re.split('_',SplitAtDot[0]))

                filename_validation_status=len(SplitAtDot[1])==length_of_datestamp and len(SplitAtDot[2])==length_of_timestamp
            else:
                filename_validation_status=False

            return filename_validation_status      
        except Exception as e:
            raise CustomException(e,sys) from e
    
    def validate_no_of_columns(self,file_path:str,schema_no_of_cols:int)->bool:
        try:
            dataframe=pd.read_csv(file_path)
            columns_length_validation=len(dataframe.columns)==schema_no_of_cols

            return columns_length_validation
        except Exception as e :
            raise CustomException(e,sys)
        
    def vaildate_missing_values_in_whole_columns(self,file_path:str)->bool:

            try:
                dataframe = pd.read_csv(file_path)
                no_of_columns_with_whole_null_values = 0
                for columns in dataframe:
                  if (len(dataframe[columns]) - dataframe[columns].count()) == len(
                        dataframe[columns]):  # checking null values
                    no_of_columns_with_whole_null_values += 1

                if no_of_columns_with_whole_null_values == 0:
                 missing_value_validation_status = True
                else:
                 missing_value_validation_status = False

                return missing_value_validation_status

            except Exception as e:
             raise CustomException(e, sys)
    def get_raw_batch_files_paths(self) -> List:

        try:
            raw_batch_files_names = os.listdir(self.raw_data_dir)
            raw_batch_files_paths = [os.path.join(self.raw_data_dir, raw_batch_file_name) for raw_batch_file_name
                                     in raw_batch_files_names]
            return raw_batch_files_paths

        except Exception as e:
            raise CustomException(e, sys)
        
    def move_raw_files_to_validation_dir(self, src_path: str, dest_path: str):

        try:
            os.makedirs(dest_path, exist_ok=True)
            if os.path.basename(src_path) not in os.listdir(dest_path):
                shutil.move(src_path, dest_path)
        except Exception as e:
            raise CustomException(e, sys)

    def validate_raw_files(self) -> bool:

        try:
            raw_batch_files_paths = self.get_raw_batch_files_paths()
            length_of_date_stamp, length_of_time_stamp, column_names, no_of_column = self.valuesfromschema()

            validated_files = 0
            for raw_file_path in raw_batch_files_paths:
                file_name_validation_status = self.validatefilename(
                    raw_file_path,
                    length_of_date_stamp=length_of_date_stamp,
                    length_of_time_stamp=length_of_time_stamp
                )
                column_length_validation_status = self.validate_no_of_columns(
                    raw_file_path,
                    schema_no_of_columns=no_of_column)

                missing_value_validation_status = self.vaildate_missing_values_in_whole_columns(raw_file_path)

                if (file_name_validation_status
                        and column_length_validation_status
                        and missing_value_validation_status):

                    validated_files += 1

                    self.move_raw_files_to_validation_dir(raw_file_path, self.data_validation_config.valid_data_dir)
                else:
                    self.move_raw_files_to_validation_dir(raw_file_path, self.data_validation_config.invalid_data_dir)

            validation_status = validated_files > 0

            return validation_status

        except Exception as e:
            raise CustomException(e, sys)

    def initiate_data_validation(self):

        logging.info("Entered initiate_data_validation method of Data_Validation class")

        try:
            logging.info("Initiated data validation for the dataset")
            validation_status = self.validate_raw_files()

            if validation_status:
                valid_data_dir = self.data_validation_config.valid_data_dir
                return valid_data_dir
            else:
                raise Exception("No data could be validated. Pipeline stopped.")

        except Exception as e:
            raise CustomException(e, sys) from e

        

        
    

        
