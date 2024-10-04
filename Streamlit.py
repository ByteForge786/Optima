import streamlit as st
import pandas as pd
import snowflake.connector
import time
from typing import Optional

def cortex_inference(prompt: str) -> str:
    query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', '{prompt}');"
    con = snowflake.connector.connect(
        # Connection params
    )
    result = pd.read_sql(query, con)
    con.close()
    return result.iloc[0, 0]

def check_query(query: str) -> str:
    template = """
    {query}
    Double check the Snowflake SQL query above for common mistakes, including:
    - Using NOT IN with NULL values
    - Using UNION when UNION ALL should have been used
    - Using BETWEEN for exclusive ranges
    - Data type mismatch in predicates
    - Properly quoting identifiers
    - Using the correct number of arguments for functions
    - Casting to the correct data type
    - Using the proper columns for joins
    If there are any of the above mistakes, rewrite the query. If there are no mistakes, just reproduce the original query.
    Output the final SQL query only.
    SQL Query: """
    
    prompt = template.format(query=query.replace('"', '\\"').replace("'", "\\'"))
    return cortex_inference(prompt)

def optimize_query(query: str) -> str:
    prompt = f"""
    You are a helpful assistant for analyzing and optimizing queries running on Snowflake to reduce resource consumption and improve performance.
    Please analyze and optimize the following SQL query:

    {query}

    Follow this plan:
    1. Analyze Query Structure
    2. Suggest Optimizations
    3. Provide clear reasoning for each suggested optimization, specifying which metric (e.g., execution time, data scanned) the optimization aims to improve.

    Output the optimized SQL query only.
    """
    return cortex_inference(prompt)

def run_query(query: str) -> tuple:
    con = snowflake.connector.connect(
        # Connection params
    )
    cursor = con.cursor()
    
    # Execute the query and get the query ID
    cursor.execute(query)
    query_id = cursor.sfqid
    
    # Fetch the results
    result = cursor.fetch_pandas_all()
    
    # Get the execution time from QUERY_HISTORY
    execution_time_query = f"""
    SELECT TOTAL_ELAPSED_TIME / 1000 as EXECUTION_TIME_SECONDS
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE QUERY_ID = '{query_id}'
    """
    execution_time_result = pd.read_sql(execution_time_query, con)
    execution_time = execution_time_result['EXECUTION_TIME_SECONDS'].iloc[0]
    
    cursor.close()
    con.close()
    
    return result, execution_time

def main():
    st.title("SQL Query Optimizer")

    query = st.text_area("Enter your SQL query:", height=200)
    original_execution_time = st.number_input("Original execution time (seconds):", min_value=0.0, step=0.1)

    if st.button("Optimize Query"):
        if query:
            with st.spinner("Checking query..."):
                checked_query = check_query(query)
            
            st.subheader("Checked Query")
            st.code(checked_query, language="sql")

            with st.spinner("Optimizing query..."):
                optimized_query = optimize_query(checked_query)
            
            st.subheader("Optimized Query")
            st.code(optimized_query, language="sql")

            with st.spinner("Running optimized query..."):
                result, optimized_execution_time = run_query(optimized_query)

            st.subheader("Query Results")
            st.dataframe(result)

            st.subheader("Performance Comparison")
            st.write(f"Original execution time: {original_execution_time:.2f} seconds")
            st.write(f"Optimized execution time: {optimized_execution_time:.2f} seconds")
            improvement = (original_execution_time - optimized_execution_time) / original_execution_time * 100
            st.write(f"Improvement: {improvement:.2f}%")

        else:
            st.warning("Please enter a SQL query.")

if __name__ == "__main__":
    main()
