import os
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
import sys
from sklearn.compose import ColumnTransformer
from src.Utils.utils import MainUtils

from sklearn.impute import SimpleImputer
from  sklearn.preprocessing import  RobustScaler,FunctionTransformer,OneHotEncoder,StandardScaler
from sklearn.pipeline import Pipeline
from imblearn.over_sampling import RandomOverSampler

from src.exception import CustomException
from src.logger import logging
from dataclasses import dataclass
from src.constants import *
from dotenv import load_dotenv
load_dotenv()

@dataclass
class DataTransformationConfig:
    Data_Transformation_dir=os.path.join('artifacts','data_transformation')
    Transformation_train=os.path.join(Data_Transformation_dir,'train.npy')
    Transformation_test=os.path.join(Data_Transformation_dir,'test.npy')
    Transformed_file=os.path.join(Data_Transformation_dir,'Preprocessing.pkl')

class DataTransformation:
    def __init__(self,valid_data_dir):
        self.data_config=DataTransformationConfig()
        self.valid_data_dir=valid_data_dir
        self.utils=MainUtils()

    @staticmethod
    def merged_batch_data(valid_data_dir:str)->pd.DataFrame:
        #iterating over thr dir and get read the data and get the dataframe 
        try:
            raw_files=os.listdir(valid_data_dir)
            CSV_DATA=[]
            
            for file in raw_files:
                data=pd.read_csv(os.path.join(valid_data_dir,file))
                CSV_DATA.append(data)
            merged_data=pd.concat(CSV_DATA)
            return merged_data
        except Exception as e:
            raise CustomException(e,sys) from e
        
    def initiate_data_transformation(self):
        logging.info('Entering into data transformation fucntion..!')

        try:
            dataframe=DataTransformation.merged_batch_data(valid_data_dir=self.valid_data_dir)
            dataframe=self.utils.remove_unwanted_space(dataframe)
            dataframe.replace('?',np.nan,inplace=True)

            #splitting
            # Convert target values -1 → 0
            dataframe[TARGET_COLUMNS] = np.where(
            dataframe[TARGET_COLUMNS] == -1, 0, 1)

            X=dataframe.drop(columns=TARGET_COLUMNS)
            Y=dataframe[TARGET_COLUMNS]

            #class imbalance 
            Sampler=RandomOverSampler()
            sampler_x,sampler_y=Sampler.fit_resample(X,Y)

            #train test split
            X_train,X_test,Y_train,Y_test=train_test_split(sampler_x,sampler_y,test_size=0.2,random_state=42)

            #impute missing values
            preprocessor=SimpleImputer(strategy='most_frequent')
            X_train_scaled=preprocessor.fit_transform(X_train)
            X_test_scaled=preprocessor.transform(X_test)
            print(X_train_scaled.shape,X_test_scaled.shape,Y_train.shape,Y_test.shape)


            preprocessor_path=self.data_config.Transformed_file
            os.makedirs(os.path.dirname(preprocessor_path),exist_ok=True)
            self.utils.save_object(file_path=preprocessor_path,
                                   obj=preprocessor)
            return X_train_scaled,X_test_scaled,Y_train,Y_test,preprocessor_path
        except Exception as e:
            raise CustomException(e,sys) from e









