# Contents of ~/my_app/pages/page_2.py
import streamlit as st
from lib.snf_functions import get_databases, get_schemas, get_tables, get_columns, deploy_udf, check_udfs, connect_to_snf, disconnect_snf

def dispaly_disconnect():
    st.write("""
    Everything is set up for running Monte Carlo simulations.

    Choose **Run Monte Carlo simulations** in the sidebar to continue.
    """)
    with st.form('Snowflake Connection'):
        st.form_submit_button('Disconnect', on_click=disconnect_snf)

st.markdown("# ❄️ Snowflake Connection")
st.sidebar.markdown("# Snowflake Connection ❄️")

if "snowsession" not in st.session_state:
    with st.form('Snowflake Credentials'):
        st.text_input('Snowflake account', key='snow_account')
        st.text_input('Snowflake user', key='snow_user')
        st.text_input('Snowflake password', key='snow_password', type='password')
        st.text_input('Snowflake warehouse', key='snow_wh')
        st.form_submit_button('Connect', on_click=connect_to_snf)
        st.stop()
else:
    if st.session_state['snowsession']:
        if 'install_schema' not in st.session_state:
            snf_session = st.session_state['snowsession']
            st.write("""
            You are now connected to your Snowflake account!
            
            Select the database and schema where the UDFs for doing Monte Carlo Simulations exists in or to be installed in 
            """)
            lst_databases = get_databases()
            sel_db = st.selectbox("Database", lst_databases)
            if sel_db:
                lst_schemas = get_schemas(sel_db)
                st.session_state['install_db'] = sel_db
                snf_session.use_database(sel_db)
            else:
                lst_schemas = []

            sel_schema = st.selectbox("Schema", options=lst_schemas)

            if sel_schema:
                st.session_state['install_schema'] = sel_schema
                snf_session.use_schema(sel_schema)
                if check_udfs(sel_db, sel_schema):
                    dispaly_disconnect()
                else:
                    st.write("""
                    The selected database and schema is missing the UDFs needed for doing the Monte Carlo simulations.
                    
                    Set the stage name for the internal stage to be used for deployment, if it does not exists it will be created. 
                    """
                    )
                    with st.form('Deploy UDfs'):
                        stage_nm = st.text_input(label="Stage name", value="MCS_STAGE", key="install_stage")
                        st.form_submit_button('Deploy', on_click=deploy_udf)
                        st.stop()
        else:
            dispaly_disconnect()
