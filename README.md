# House_Price_Prediction_GL

## Explanation of the Model built

All python scripts use the configuration.txt file 

1.  directory -->> load_csv_to_mongo

     Standalone, independent code to upload the train and test csv files to MongoDB hosted on an AWS machine.

2.  Created a base Pyspark image with all the required dependencies.

    DOCKER BASE IMAGE: gaddamsrikanth24/house-prices:latest

3.  Created a docker volume - "house_prices" and using "/usr/src/app/" as the destination directory for persisiting all the data.

4. Fetch data from MongoDB and write to a csv file on docker volume

   Python Code: fetch_data_from_db.py
   
   Dockerfile: fetch_data_from_DB.dockerfile
   
   DockerImage: gaddamsrikanth24/house-prices:fetch_data_from_db
   
   Docker RUN Command: docker run --mount source=house_prices,destination=/usr/src/app gaddamsrikanth24/house-prices:fetch_data_from_db


5. Create a pipeline with all data preprocessing transformations in place and export a pipeline.

   Python Code: data_preprocess_pipeline.py
   
   Dockerfile: data_preprocess_pipeline.dockerfile
   
   DockerImage: gaddamsrikanth24/house-prices:data_preprocess_pipeline
   
   Docker RUN Command: docker run --mount source=house_prices,destination=/usr/src/app gaddamsrikanth24/house-prices:data_preprocess_pipeline
   

6. Load pipeline from last step, apply it on data fetched from DB.

   Python Code: data_preprocess.py
   
   Dockerfile: data_preprocess.dockerfile
   
   DockerImage: gaddamsrikanth24/house-prices:data_preprocess
   
   Docker RUN Command: docker run --mount source=house_prices,destination=/usr/src/app gaddamsrikanth24/house-prices:data_preprocess
   

7. Create a feature process pipeline fitted on preprocessed data, and export.

   Python Code: feature_processing_pipeline.py
   
   Dockerfile: feature_processing_pipeline.dockerfile
   
   DockerImage: gaddamsrikanth24/house-prices:feature_processing_pipeline
   
   Docker RUN Command: docker run --mount source=house_prices,destination=/usr/src/app gaddamsrikanth24/house-prices:feature_processing_pipeline
   

8. Load feature modelling and scaling pipeline from last step, apply it on preprocessed data.

    * Here, we are also using GBTRegressor model with hyper-parameters to generate a predicted data CSV file.
    
   Python Code: scale_model.py
   
   Dockerfile: scale_model.dockerfile
   
   DockerImage: gaddamsrikanth24/house-prices:scale_model
   
   Docker RUN Command: docker run --mount source=house_prices,destination=/usr/src/app gaddamsrikanth24/house-prices:scale_model
   
   
9. Evaluate metrics from the CSV file generated in the previous step.

   Python Code: eval_metrics.py
   
   Dockerfile: eval_metrics.dockerfile
   
   DockerImage: gaddamsrikanth24/house-prices:eval_metrics
   
   Docker RUN Command: docker run --mount source=house_prices,destination=/usr/src/app gaddamsrikanth24/house-prices:eval_metrics
  
  
------------------------------------------------------
TESTING  -- Testing with data loaded on MongoDB
------------------------------------------------------

1. Fetch the test data.

   Python Code: fetch_test_data.py
   
   Dockerfile: fetch_test_data.dockerfile
   
   DockerImage: gaddamsrikanth24/house-prices:fetch_test_data
   
   Docker RUN Command: docker run --mount source=house_prices,destination=/usr/src/app gaddamsrikanth24/house-prices:fetch_test_data
  
2. Load the test data, transform with all the pipelines and apply the Regressor model from previously trained data.

   Python Code: predict_prices.py
   
   Dockerfile: predict_prices.dockerfile
   
   DockerImage: gaddamsrikanth24/house-prices:predict_prices
   
   Docker RUN Command: docker run --mount source=house_prices,destination=/usr/src/app gaddamsrikanth24/house-prices:predict_prices
