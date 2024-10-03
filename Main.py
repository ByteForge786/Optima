import snowflake.connector
import pandas as pd
import streamlit as st
from cortex_inference import cortex_inference
from agent import run_agent

@st.cache_resource(ttl='5h')
def get_db(username, password, account, warehouse, role):
    database = "SNOWFLAKE"
    schema = "ACCOUNT_USAGE"
    con = snowflake.connector.connect(
        user=username,
        password=password,
        account=account,
        database=database,
        schema=schema,
        warehouse=warehouse,
        role=role,
    )
    return con


st.set_page_config(page_title="Snow-Wise", page_icon="❄️")
st.title("❄️ :blue[Snow-Wise]")

st.write('AI agent to monitor & optimize Snowflake queries :rocket:')

with st.sidebar:
    st.title('Your Secrets')
    st.caption('Please use a role with SNOWFLAKE database privileges ([docs](https://docs.snowflake.com/en/sql-reference/account-usage#enabling-the-snowflake-database-usage-for-other-roles))')
    snowflake_account= st.text_input("Snowflake Account", key="snowflake_account")
    snowflake_username= st.text_input("Snowflake Username", key="snowflake_username")
    snowflake_password= st.text_input("Snowflake Password", key="snowflake_password", type="password")
    snowflake_warehouse= st.text_input("Snowflake Warehouse", key="snowflake_warehouse")
    snowflake_role= st.text_input("Snowflake Role", key="snowflake_role")

    if snowflake_account and snowflake_username and snowflake_role and snowflake_password and snowflake_warehouse:
        con = get_db(
            username=snowflake_username,
            password=snowflake_password,
            account=snowflake_account,
            warehouse=snowflake_warehouse,
            role=snowflake_role,
        )


if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("I need help with finding the long running queries on my Snowflake"):
    if not (snowflake_account and snowflake_username and snowflake_role and snowflake_password and snowflake_warehouse):
        st.info("Please add the secrets to continue!")
        st.stop()

    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        response = run_agent(con, prompt)
        st.markdown(response)

    st.session_state.messages.append({"role": "assistant", "content": response})
