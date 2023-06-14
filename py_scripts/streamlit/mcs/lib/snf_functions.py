import streamlit as st

from scipy.stats import norm
import numpy as np
from typing import Tuple, Iterable

from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T


def connect_to_snf():
    if 'snowsession' in st.session_state:
        return st.session_state['snowsession']

    creds = {
        'account': st.session_state['snow_account'],
        'user': st.session_state['snow_user'],
        'password': st.session_state['snow_password'],
        'warehouse': st.session_state['snow_wh']
    }
    session = Session.builder.configs(creds).create()
    st.session_state['snowsession'] = session

    return session


def disconnect_snf():
    if 'snowsession' in st.session_state:
        session = st.session_state['snowsession']
        session.close()
        del st.session_state['snowsession']
        del st.session_state['install_db']
        del st.session_state['install_schema']
        del st.session_state['install_stage']

@st.cache_data()
def check_udfs(data_db: str, data_schema: str):
    snf_session = st.session_state['snowsession']
    udf_funcs = ['NORM_PPF', 'COLLECT_LIST', 'CALC_CLOSE']

    n_udfs = snf_session.table(f"{data_db}.INFORMATION_SCHEMA.FUNCTIONS").filter(
        (F.col("FUNCTION_SCHEMA") == F.lit(data_schema)) & (F.col("FUNCTION_NAME").in_(udf_funcs))).count()

    if n_udfs == len(udf_funcs):
        st.session_state['install_stage'] = ''
        return True
    else:
        return False

def deploy_udf():
    snf_session = st.session_state['snowsession']
    data_db = st.session_state['install_db']
    data_schema = st.session_state['install_schema']
    stage_name = st.session_state['install_stage']

    stage_loc = data_db + '.' + data_schema + '.' + stage_name
    # Check for stage
    n_stages = snf_session.table(f"{data_db}.INFORMATION_SCHEMA.STAGES").filter(
        (F.col("STAGE_SCHEMA") == F.lit(data_schema)) & (F.col("STAGE_NAME") == F.lit(stage_name))).count()
    if n_stages == 0:
        snf_session.sql(f"CREATE STAGE IF NOT EXISTS {stage_name}").collect()

    @F.udf(name=f"{data_db}.{data_schema}.norm_ppf", is_permanent=True, replace=True, packages=["scipy"],
           stage_location=stage_loc)
    def norm_ppf(pd_series: T.PandasSeries[float]) -> T.PandasSeries[float]:
        return norm.ppf(pd_series)

    @F.udtf(name=f"{data_db}.{data_schema}.collect_list", is_permanent=True, replace=True
        , packages=["typing"], output_schema=T.StructType([T.StructField("list", T.ArrayType())])
        , stage_location=stage_loc)
    class CollectListHandler:
        def __init__(self) -> None:
            self.list = []

        def process(self, element: float) -> Iterable[Tuple[list]]:
            self.list.append(element)
            yield (self.list,)

    @F.udf(name=f"{data_db}.{data_schema}.calc_close", is_permanent=True, replace=True
        , packages=["numpy"], stage_location=stage_loc)
    def calc_return(last_close: float, daily_return: list) -> float:
        pred_close = last_close * np.prod(daily_return)
        return float(pred_close)

    return True


@st.cache_data()
def get_databases():
    snf_session = st.session_state['snowsession']
    lst_db = [dbRow[1] for dbRow in snf_session.sql("SHOW DATABASES").collect()]
    # Add a default None value
    lst_db.insert(0, None)
    return lst_db


@st.cache_data()
def get_schemas(db: str):
    snf_session = st.session_state['snowsession']
    lst_schema = [schemaRow[0] for schemaRow in snf_session.sql(
        f"SELECT SCHEMA_NAME FROM {db}.INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME = '{db.upper()}' AND SCHEMA_NAME != 'INFORMATION_SCHEMA' ORDER BY 1").collect()]
    lst_schema.insert(0, None)
    return lst_schema


@st.cache_data()
def get_tables(db: str, schema: str):
    snf_session = st.session_state['snowsession']
    lst_table = [tableRow[0] for tableRow in snf_session.sql(
        f"SELECT TABLE_NAME FROM {db}.INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '{db.upper()}' AND TABLE_SCHEMA='{schema.upper()}' ORDER BY 1").collect()]
    lst_table.insert(0, None)
    return lst_table


@st.cache_data()
def get_columns(db: str, schema: str, table: str):
    snf_session = st.session_state['snowsession']
    lst_column = [columnRow[0] for columnRow in snf_session.sql(
        f"SELECT COLUMN_NAME, DATA_TYPE FROM {db}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '{db.upper()}' AND TABLE_SCHEMA='{schema.upper()}' AND TABLE_NAME = '{table.upper()}' ORDER BY 1").collect()]
    return lst_column
