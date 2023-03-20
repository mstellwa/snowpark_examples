###################################################################################
# Setup script for the MLFlow examples
#
# 2022-12-22 Mats Stellwall, Snowflake
##################################################################################

import warnings
import json
import joblib
import io
import logging
import sys

from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F

import pandas as pd
import numpy as np

from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    warnings.filterwarnings("ignore")

    if len(sys.argv) < 5:
        print('Too few arguments')

    creds_file = sys.argv[1] # 'creds.json'
    sp_name = sys.argv[2]
    data_table = sys.argv[3]
    stage_name = sys.argv[4]

    # Read the wine-quality csv file from the URL
    csv_url = (
        "https://raw.githubusercontent.com/mlflow/mlflow/master/tests/data/winequality-red.csv"
    )
    try:
        data = pd.read_csv(csv_url, sep=";")
    except Exception as e:
        logger.exception(
            "Unable to download training & test CSV, check your internet connection. Error: %s", e
        )

    # rename columns to replace space with underscore
    data = data.rename(columns={a:a.replace(' ', '_') for a in data.columns})

    # Connect to Snowflake
    with open(creds_file) as f:
        connection_parameters = json.load(f)

    session = Session.builder.configs(connection_parameters).create()

    df_wine = session.write_pandas(data, table_name=data_table, quote_identifiers=False, auto_create_table=True, overwrite=True)

    def save_file_to_stage(snf_session, obj, path):
        input_stream = io.BytesIO()
        joblib.dump(obj, input_stream)
        snf_session._conn._cursor.upload_stream(input_stream, path)
        return path


    def eval_metrics(actual, pred):
        rmse = np.sqrt(mean_squared_error(actual, pred))
        mae = mean_absolute_error(actual, pred)
        r2 = r2_score(actual, pred)
        return rmse, mae, r2

    def map_dtype(dtype):
        if dtype.kind == "b":
            return "boolean"
        elif dtype.kind == "i" or dtype.kind == "u":
            if dtype.itemsize < 4 or (dtype.kind == "i" and dtype.itemsize == 4):
                return "integer"
            elif dtype.itemsize < 8 or (dtype.kind == "i" and dtype.itemsize == 8):
                return "long"
        elif dtype.kind == "f":
            if dtype.itemsize <= 4:
                return "float"
            elif dtype.itemsize <= 8:
                return "double"

        elif dtype.kind == "U":
            return "string"
        elif dtype.kind == "S":
            return "binary"
        elif dtype.kind == "M":
            return "datetime"


    def train_model(snf_session: Session, train_table: str, model_param: dict) -> dict:

        train_alpha = float(model_param['alpha'])
        train_l1_ratio = float(model_param['l1_ratio'])
        train_data = snf_session.table(train_table).to_pandas()
        train, test = train_test_split(train_data)
        train_x = train.drop(["QUALITY"], axis=1)
        test_x = test.drop(["QUALITY"], axis=1)
        train_y = train[["QUALITY"]]
        test_y = test[["QUALITY"]]

        lr = ElasticNet(alpha=train_alpha, l1_ratio=train_l1_ratio, random_state=42)
        lr.fit(train_x, train_y)
        predicted_qualities = lr.predict(test_x)
        (rmse, mae, r2) = eval_metrics(test_y, predicted_qualities)

        model_path = save_file_to_stage(snf_session, lr, f'@{stage_name}/mlflow_test/lr_model.joblib')

        # Get the model signature, so we can deploy it later to Snowflake
        input_list = [{"type": map_dtype(test_x[col].dtype), "name": col} for col in test_x.columns]
        output_list = [{"type": map_dtype(test_y[col].dtype), "name": col} for col in test_y.columns]

        ret_dict = {"alpha": train_alpha,
                    "l1_ratio": train_l1_ratio,
                    "rmse": rmse,
                    "r2": r2,
                    "mae": mae,
                    "model_path": model_path,
                    "sign_dict": {"inputs": input_list, "outputs": output_list}}
        return ret_dict


    session.clear_imports()
    session.clear_packages()

    train_model_sp = F.sproc(train_model, name=sp_name, is_permanent=True, session = session,stage_location=stage_name, replace=True,
                             packages=['snowflake-snowpark-python', 'scikit-learn', 'pandas', 'numpy'])

    print(f'Deployed Stored Procedure: {sp_name}')
