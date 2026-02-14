from flask import Flask, jsonify,render_template,send_file,request
from src.exception import CustomException
import sys
from src.logger import logging
from src.pipelines.Prediction import PredictPipeline
from src.pipelines.Training import TrainingPipeline

app=Flask(__name__)

@app.route("/")
def home():
   return  render_template('index.html')

@app.route("/train",methods=['GET'])
def train():
   try:
      train_pipeline=TrainingPipeline()
      train_pipeline_detail=train_pipeline.run_pipeline()
      logging.info('Training completed..')
      return send_file(train_pipeline_detail,download_name='model.pkl',as_attachment=True)
   except Exception as e:
      raise CustomException(e,sys) from e  
@app.route("/predict",methods=['Get','POST'])
def predict():
   try:
      if request.method=='POST' :
         predict=PredictPipeline(request)
         predict_file_detail=predict.run_pipeline()
         logging.info('prediction completed ,downloading prediction file..')
         return send_file(predict_file_detail.prediction_file_path,download_name=predict_file_detail.prediction_file_output,as_attachment=True)
      
      else:
         return render_template("prediction.html")
   except Exception as e:
      raise CustomException(e,sys) from e


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug= True)

   
