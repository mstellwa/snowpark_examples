###################################################################################
# Example of how to deploy a model with MLFlow to Snowflake.
# In order to run this https://github.com/Snowflake-Labs/mlflow-snowflake needs to be installed
#
# 2022-12-22 Mats Stellwall, Snowflake
##################################################################################
import sys
import json
import warnings

from snowflake.ml.mlflow import create_session
from mlflow.deployments import get_deploy_client


import logging

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    creds_file = sys.argv[1]  # 'creds.json'
    udf_name = sys.argv[2]
    udf_stage = sys.argv[3]
    model_path = sys.argv[4]

    with open(creds_file) as f:
        connection_parameters = json.load(f)

    create_session(connection_parameters)
    target_uri = 'snowflake'
    deployment_client = get_deploy_client(target_uri)

    # Creates a permanent UDF that uses the provided Stage
    ret = deployment_client.create_deployment(udf_name, model_path, flavor='sklearn', config={"persist_udf_file":True, 'stage_location':f'@{udf_stage}'})

    print(f"Model deployed as MLFLOW${udf_name.upper()}")