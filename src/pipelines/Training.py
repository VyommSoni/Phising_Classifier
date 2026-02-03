import os
import numpy as np
import pandas as pd
import pathlib
from src.components.Data_ingestion import DataIngestion
from src.components.Data_Validation import DataValidation
from src.components.Data_Transformation import DataTransformation
from src.components.Model_Trainer import ModelTrainer
from src.exception import CustomException
from src.logger import logging
import sys


class TrainingPipeline:

    def start_data_ingestion(self):
        logging.info('Entering into Data ingestion function..')
        try:
            data_ingestion=DataIngestion()
            raw_data_dir=data_ingestion.initiate_data_ingestion()
            return raw_data_dir
        except Exception as e:
            raise CustomException (e,sys)
        
    def start_Data_validation(self,raw_data_dir):
        try:
            logging.info('Entering into Data Validation fucntion..')
            data_validation=DataValidation(raw_data_store_dir=raw_data_dir)
            valid_data_dir=data_validation.initiate_data_validation()
            return valid_data_dir
        except Exception as e:
            raise CustomException(e,sys) from e
        
    def Data_Transformation(self,valid_data_dir):
        try:
            logging.info('Entering into Data Transformation function..')
            data_transformation=DataTransformation(valid_data_dir=valid_data_dir)
            X_train,Y_train,X_test,Y_test,preprocessor_path=data_transformation.initiate_data_transformation()
            return  X_train,Y_train,X_test,Y_test,preprocessor_path
        except Exception as e:
            raise CustomException(e,sys) from e
        
    def start_model_training(self,
                             X_train:np.array,
                             Y_train:np.array,X_test:np.array,
                             Y_test:np.array,
                             preprocessor_path:pathlib.Path):
        try:
            logging.info('Entering into model training function..')
            model_trainer=ModelTrainer()

            model_score=model_trainer.initiate_model_trainer(
                X_train, Y_train,X_test,Y_test,preprocessor_path
            )
            return model_score,model_trainer
        except Exception as e:
            raise CustomException(e,sys) from e
    
    def run_pipeline(self):
        try:
            logging.info('Entering into Run pipeline fucntion...')
            raw_data_dir=self.start_data_ingestion()
            valid_data_dir=self.start_Data_validation(raw_data_dir)
            X_train,Y_train,X_test,Y_test,preprocessor_path=self.Data_Transformation(valid_data_dir)
            accuracy,model_trainer=self.start_model_training(X_train,Y_train,X_test,Y_test,preprocessor_path)

            print('Training completed,accuracy of model is ',accuracy)

            return model_trainer
        except Exception as e:
            raise CustomException(e,sys) from e
if __name__=='__main__':
    train_pipeline=TrainingPipeline()
    train_pipeline.run_pipeline()
    

