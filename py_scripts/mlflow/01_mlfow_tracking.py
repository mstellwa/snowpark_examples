###################################################################################
# Example of how to do training in Snowflake using a SP and track it in MLFlow
#
# 2022-12-22 Mats Stellwall, Snowflake
##################################################################################

import warnings
import sys

import joblib
import json
import numpy as np

from urllib.parse import urlparse
import mlflow
import mlflow.sklearn
from mlflow.models import ModelSignature

from snowflake.snowpark.session import Session

import logging

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    warnings.filterwarnings("ignore")

    if len(sys.argv) < 4:
        print('Too few arguments')

    creds_file = sys.argv[1]
    sp_name = sys.argv[2]
    input_table = sys.argv[3]
    alpha = float(sys.argv[4]) if len(sys.argv) > 4 else 0.5
    l1_ratio = float(sys.argv[5]) if len(sys.argv) > 5 else 0.5

    np.random.seed(40)

    with open(creds_file) as f:
        connection_parameters = json.load(f)

    session = Session.builder.configs(connection_parameters).create()

    params = {"alpha": alpha, "l1_ratio": l1_ratio}

    with mlflow.start_run():
        training_dict = json.loads(session.call(sp_name, input_table, params))  # Workaround for now
        rmse = training_dict['rmse']
        mae = training_dict['mae']
        r2 = training_dict['r2']

        print("Elasticnet model (alpha={:f}, l1_ratio={:f}):".format(alpha, l1_ratio))
        print("  RMSE: %s" % rmse)
        print("  MAE: %s" % mae)
        print("  R2: %s" % r2)

        mlflow.log_param("alpha", alpha)
        mlflow.log_param("l1_ratio", l1_ratio)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mae", mae)

        # Pull the model from Snowflake
        output_stream = session.file.get_stream(training_dict['model_path'])
        trained_model = joblib.load(output_stream)

        # Get the model Signature
        signature = ModelSignature.from_dict({'inputs': json.dumps(training_dict['sign_dict']['inputs']),
                                              'outputs': json.dumps(training_dict['sign_dict']['outputs'])})
        tracking_url_type_store = urlparse(mlflow.get_tracking_uri()).scheme
        # Model registry does not work with file store
        if tracking_url_type_store != "file":

            # Register the model
            # There are other ways to use the Model Registry, which depends on the use case,
            # please refer to the doc for more information:
            # https://mlflow.org/docs/latest/model-registry.html#api-workflow
            mlflow.sklearn.log_model(trained_model, "model", registered_model_name="ElasticnetWineModel",  signature=signature)
        else:
            mlflow.sklearn.log_model(trained_model, "model", signature=signature)

    session.close()
