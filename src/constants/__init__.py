from datetime import datetime
import os

MODEL_EXTENSTION=".pkl"
MODEL_NAME='model'

AWS_S3_BUCKET_NAME="phisingclassifierbucket"
TARGET_COLUMNS='Result'
MONGO_DATABASE_NAME='Phising'

Artifact_Folder_Name=datetime.now().strftime("%m_%d_%y_%H_%M_%S")
Artifact_Folder=os.path.join('artifact',Artifact_Folder_Name)
#in this constant folder ,we use it for project in which the name of files,
#or etc will remain same throughout the project
