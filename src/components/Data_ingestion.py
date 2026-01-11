import os
import sys
import numpy as np
import pandas as pd
import pymongo as MongoClient
from zipfile import Path
from src.data_access.phising_data import PhisingData
from dataclasses import dataclass
from src.logger import logging
from src.exception import CustomException
from src.Utils import utils
from src.constants import *



#we want to store data in our local system after reading the data
@dataclass
class DataIngestionConfig:
    data_ingestion_dir: str = os.path.join('artifacts', "data_ingestion")


class DataIngestion:
    def __init__(self):

        self.data_ingestion_config = DataIngestionConfig()
        self.utils = utils()

    def export_data_into_raw_data_dir(self) -> pd.DataFrame:
        try:
            logging.info(f"Exporting data from mongodb")
            raw_batch_files_path = self.data_ingestion_config.data_ingestion_dir
            os.makedirs(raw_batch_files_path, exist_ok=True)

            income_data = PhisingData(
                database_name=MONGO_DATABASE_NAME)

            logging.info(f"Saving exported data into feature store file path: {raw_batch_files_path}")
            for collection_name, dataset in income_data.export_collections_as_dataframe():
                logging.info(f"Shape of {collection_name}: {dataset.shape}")
                feature_store_file_path = os.path.join(raw_batch_files_path, collection_name + '.csv')
                print(f"feature_store_file_path-----{feature_store_file_path}")
                # dataset.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                dataset.to_csv(feature_store_file_path, index=False)


        except Exception as e:
            raise CustomException(e, sys)

    def initiate_data_ingestion(self) -> Path:
        logging.info("Entered initiate_data_ingestion method of Data_Ingestion class")

        try:
            self.export_data_into_raw_data_dir()

            logging.info("Got the data from mongodb")

            logging.info(
                "Exited initiate_data_ingestion method of Data_Ingestion class"
            )

            return self.data_ingestion_config.data_ingestion_dir

        except Exception as e:
            raise CustomException(e, sys) from e
