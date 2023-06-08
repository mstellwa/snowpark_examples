import streamlit as st

st.title("Monte Carlo Simulations :spades:")
st.write(
    """ 
    A demo showing how Monte Carlo simulations can be used to predict the future stock price for P&G.

    A Monte Carlo simulation is a mathematical technique, which is used to estimate the possible outcomes of an uncertain event. 
    A Monte Carlo analysis consists of input variables, output variables, and a mathematical model. 

    This demo is using the following mathematical model:

             Stock Price Today = Stock Price Yesterday * e^r

    To calculate r the geometric Brownian motion (GBM) model is used.

    Start by connecting to your Snowflake account, using the **Snowflake connect** link in the sidebar.

    """
)

