import streamlit as st

import snowflake.snowpark.functions as F
from snowflake.snowpark import Column, Window

from lib.snf_functions import get_databases, get_schemas, get_tables, get_columns, deploy_udf

import plotly.express as px

# Get the current credentials
if "snowsession" in st.session_state:
    snf_session = st.session_state['snowsession']
else:
    st.write("**Please log into you Snowflake account first!**")
    st.stop()
def run_simulations(df, n_days, n_sim_runs):
    def pct_change(indx_col: Column, val_col: Column):
        return ((val_col - F.lag(val_col, 1).over(Window.orderBy(indx_col))) / F.lag(val_col, 1).over(
            Window.orderBy(indx_col)))

    # Calculate the log return by day
    df_log_returns = df_closing.select(F.col("DATE"), F.col("CLOSE")
                                       , F.last_value(F.col("CLOSE")).over(Window.orderBy("DATE")).as_("LAST_CLOSE")
                                       , F.call_function("LN",
                                                         (F.lit(1) + pct_change(F.col("DATE"), F.col("CLOSE")))).as_(
            "log_return"))

    # Get the u, var, stddev and last closing price
    df_params = df_log_returns.select(F.mean("LOG_RETURN").as_("u")
                                      , F.variance("LOG_RETURN").as_("var")
                                      , F.stddev("LOG_RETURN").as_("std_dev")
                                      , F.max(F.col("LAST_CLOSE")).as_("LAST_CLOSE")) \
        .with_column("drift", (F.col("u") - (F.lit(0.5) * F.col("var")))) \
        .select("std_dev", "drift", "last_close")

    # Generates rows for the number of days and simulations by day
    df_days = snf_session.generator(F.row_number().over(Window.order_by(F.seq4())).as_("day_id"), rowcount=n_days)
    df_sim_runs = snf_session.generator(F.row_number().over(Window.order_by(F.seq4())).as_("sim_run"),
                                        rowcount=n_sim_runs)

    df_daily_returns = df_days.join(df_sim_runs).join(df_params) \
        .select("day_id", "sim_run"
                , F.exp(F.col("drift") + F.col("std_dev") * F.call_function(f"{data_db}.{data_schema}.norm_ppf",
                                                                            F.uniform(0.0, 1.0, F.random()))).as_(
            "daily_return")
                , F.lit(None).as_("SIM_CLOSE")) \
        .sort(F.col("DAY_ID"), F.col("sim_run"))

    # Generate a day 0 row with the last closing price for each simulation run
    last_close = df_params.select("LAST_CLOSE").collect()[0][0]
    df_day_0 = snf_session.generator(F.lit(0).as_("DAY_ID"),
                                     F.row_number().over(Window.order_by(F.seq4())).as_("SIM_RUN")
                                     , F.lit(1.0).as_("DAILY_RETURN"), F.lit(last_close).as_("SIM_CLOSE"),
                                     rowcount=n_sim_runs)

    # Union the dataframes,
    df_simulations = df_day_0.union_all(df_daily_returns)

    df_simulations_calc_input = df_simulations.with_column("SIM_CLOSE_0", F.first_value(F.col("SIM_CLOSE")).over(
        Window.partition_by("SIM_RUN").order_by("DAY_ID"))) \
        .with_column("L_DAILY_RETURN",
                     F.call_table_function(f"{data_db}.{data_schema}.collect_list", F.col("DAILY_RETURN")).over(
                         partition_by="SIM_RUN", order_by="DAY_ID"))

    df_sim_close = df_simulations_calc_input.with_column("SIM_CLOSE",
                                                         F.call_function(f"{data_db}.{data_schema}.calc_close"
                                                                         , F.col("SIM_CLOSE_0"),
                                                                         F.col("L_DAILY_RETURN")))

    # Cache the returning Snowpark Dataframe so we do not run it multiple times when visulazing etc
    return df_sim_close.select("DAY_ID", "SIM_RUN", "SIM_CLOSE").cache_result()


def display_sim_result(df):
    pd_simulations = df.sort("DAY_ID", "SIM_RUN").to_pandas()

    fig = px.line(pd_simulations, x="DAY_ID", y="SIM_CLOSE", color='SIM_RUN', render_mode='svg')
    st.plotly_chart(fig, use_container_width=True)
    metrics = df.select(F.round(F.mean(F.col("SIM_CLOSE")), 2)
                        , F.round(F.percentile_cont(0.05).within_group("SIM_CLOSE"), 2)
                        , F.round(F.percentile_cont(0.95).within_group("SIM_CLOSE"), 2)).collect()
    st.write("Expected price: ", metrics[0][0])
    st.write(f"Quantile (5%): ", metrics[0][1])
    st.write(f"Quantile (95%): ", metrics[0][2])


# Write directly to the app
st.sidebar.markdown("# Simulation Parameters")
st.title("Monte Carlo Simulations :balloon:")
st.write("Start by choosing the table and columns with the date and stock prices that is going to be used for the simulations.")

with st.sidebar:
    with st.form(key="simulation_param"):
        n_days = st.slider('Number of Days to Generate', 1, 1000, 100)
        n_iterations = st.slider('Number of Simulations by Day', 1, 100, 20)
        st.session_state.start_sim_clicked = st.form_submit_button(label="Run Simulations")

lst_databases = get_databases()
col1, col2, col3, col4 = st.columns(4)
sel_db = col1.selectbox("Database", lst_databases)
if sel_db:
    lst_schemas = get_schemas(sel_db)
    snf_session.use_database(sel_db)
else:
    lst_schemas = []

sel_schema = col2.selectbox("Schema", options=lst_schemas)

if sel_schema:
    lst_tables = get_tables(sel_db, sel_schema)
    snf_session.use_schema(sel_schema)
else:
    lst_tables = []

sel_table = col3.selectbox("Table", lst_tables)
if sel_table:
    lst_columns = get_columns(sel_db, sel_schema, sel_table)
else:
    lst_columns = []

sel_columns = col4.multiselect("Columns", lst_columns, max_selections=2)
if len(sel_columns) == 2:
    df_closing = snf_session.table(f"{sel_db}.{sel_schema}.{sel_table}").select(F.col(sel_columns[0]).as_("DATE"),
                                                                                F.col(sel_columns[1]).as_("CLOSE"))
    st.write(df_closing.count())
    st.line_chart(df_closing.to_pandas(), x="DATE", y="CLOSE")

if st.session_state.start_sim_clicked:
    with st.spinner('Running simulations...'):
        df_simulations = run_simulations(df_closing, n_days, n_iterations)
        display_sim_result(df_simulations)
        st.session_state["df"] = df_simulations
        st.session_state.start_sim_clicked = False

if "df" in st.session_state:
    st.write("Choose the database and schema to save the data into:")
    save_db = st.selectbox("Database", lst_databases, key="save_db")
    if save_db:
        lst_schemas = get_schemas(save_db)
        snf_session.use_database(save_db)
    else:
        lst_schemas = []
    save_schema = st.selectbox("Schema", options=lst_schemas, key="save_schema")

    if save_schema:
        have_schema = False
    else:
        have_schema = True

    save_tbl = st.text_input(label="Table name", value="STOCK_PRICE_SIMULATIONS", disabled=have_schema)
    save = st.button("❄️ Save results", key="save_sims")
    if save:
        df_simulations = st.session_state["df"]
        display_sim_result(df_simulations)
        with st.spinner("Saving data..."):
            df_simulations.write.mode('overwrite').save_as_table(f"{save_db}.{save_schema}.{save_tbl}")
            st.success(f"✅ Successfully wrote simulations to {save_db}.{save_schema}.{save_tbl}!")
            st.session_state["saved"] = True
