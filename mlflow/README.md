# MLFlow demos with Snowpark
Simple demos of how to use MLFlow with Snowpark for Python

Currently, it only uses local tracking for MLFLow and store models as part of the run.

## Setup
Download all the files into a directory.

### Create a Python environment
Install Anaconda or miniconda
Make sure you are in the directory where you downloaded the files.
```shell
conda env create -f conda_env.yml
conda activate mlfsnowpark
```
### Load data and deploy store procedure
You need to create a JSON file, creds.json, with the following structure
```json
{
    "account":"MY SNOWFLAKE ACCOUNT",
    "user": "MY USER",
    "password":"MY PASSWORD",
    "role":"MY ROLE",
    "warehouse":"MY WH",
    "database":"MY DB",
    "schema":"MY SCHEMA"
}
```
You need to have a internal stage where to be used for stored procedures and to store trained models 
Your user needs to have the permissions to create tables and stored procedures in the database and schema you connect to.
```shell
python 00_setup.py 'my_path/creds.json' 'name of training sp' 'name of table to store data in' 'name_of_stage'
```
Once the script is done you are good to go.

## Demos
### Experiment tracking
Based on the MLFlow tutorial at https://www.mlflow.org/docs/latest/tutorials-and-examples/tutorial.html

Run the 01_mlflow_tracking.py file to run one experiment that trains a model (Elasticnet model) and store it in the local MLFlow repository.

You need to provide the creds.json file, the name of the training sp and the table with training data.
You can provide alpha and l1_ratio, if not it will use 0.5 as the default
```shell
python 01_mlflow_tracking.py 'my_path/creds.json' 'name of training sp' 'name of table with training data' <alpha> <l1_ratio>
```
You can view the experiment in the MLFlow GUI, first make sure you are in the directory that has the mlruns folder. 
```shell
mlflow ui
```
### Deploy model from MLFlow repository to Snowflake

This demo uses https://github.com/Snowflake-Labs/mlflow-snowflake, please follow the instructions there to install it.

In order to deploy a model you need to get the path to the model that was trained and logged with 01_mlflow_tracking.py.
Easiest way is to look in the mlflow GUI at the experiment and under artifacts copy the url to the model directory.
You need to get rid of the `file://` part. 
For example `file:///Users/mstellwall/Documents/GitHub/mstellwall-demos/py_code/mlflow/mlruns/0/a6783da3c7b7441aa692974deef1e45b/artifacts/model` will be provided to the script as `/Users/mstellwall/Documents/GitHub/mstellwall-demos/py_code/mlflow/mlruns/0/a6783da3c7b7441aa692974deef1e45b/artifacts/model` 


With that the 02_mlflow_deploy.py script can be called
```shell
 python 02_mlflow_deploy.py 'my_path/creds.json' 'my_udf_name' 'udf_stage_name' 'path_to_my_model' 
```
  


