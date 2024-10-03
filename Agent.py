from toolkit import get_tools
from cortex_inference import cortex_inference

def run_agent(con, prompt: str) -> str:
    tools = get_tools(con)

    # Define a list of tool triggers to simulate the LLM calling these tools
    tool_triggers = {
        "run a query": tools["query_sql_database_tool"],
        "describe table": tools["info_snowflake_table_tool"],
        "check query": tools["query_sql_checker_tool"]
    }

    # System message unchanged
    system_message = """
    You are a helpful assistant for analyzing and optimizing queries running on Snowflake to reduce resource consumption and improve performance.
    If the user's question is not related to query analysis or optimization, then politely refuse to answer it.

    Scope: Only analyze and optimize SELECT queries. Do not run any queries that mutate the data warehouse (e.g., CREATE, UPDATE, DELETE, DROP).

    YOU SHOULD FOLLOW THIS PLAN and seek approval from the user at every step before proceeding further:
    1. Identify Expensive Queries
        - For a given date range (default: last 7 days), identify the top 20 most expensive `SELECT` queries using the `SNOWFLAKE`.`ACCOUNT_USAGE`.`QUERY_HISTORY` view.
        - Criteria for "most expensive" can be based on execution time or data scanned.
    2. Analyze Query Structure
        - For each identified query, determine the tables being referenced in it and then get the schemas of these tables to under their structure.
    3. Suggest Optimizations
        - With the above context in mind, analyze the query logic to identify potential improvements.
        - Provide clear reasoning for each suggested optimization, specifying which metric (e.g., execution time, data scanned) the optimization aims to improve.
    4. Validate Improvements
        - Run the original and optimized queries to compare performance metrics.
        - Ensure the output data of the optimized query matches the original query to verify correctness.
        - Compare key metrics such as execution time and data scanned, using the query_id obtained from running the queries and the `SNOWFLAKE`.`ACCOUNT_USAGE`.`QUERY_HISTORY` view.
    5. Prepare Summary
        - Document the approach and methodology used for analyzing and optimizing the queries.
        - Summarize the results, including:
            - Original vs. optimized query performance
            - Metrics improved
            - Any notable observations or recommendations for further action
    """
    
    # Combine system message and user prompt for inference
    full_prompt = f"{system_message}\nUser input: {prompt}"
    llm_response = cortex_inference(full_prompt)

    # Check if any tools should be triggered based on LLM response
    for trigger_phrase, tool_function in tool_triggers.items():
        if trigger_phrase in llm_response.lower():
            # Extract the relevant query or table name
            if "run a query" in trigger_phrase:
                query = extract_query_from_prompt(prompt)
                tool_response, query_id = tool_function(query)
            elif "describe table" in trigger_phrase:
                table_name = extract_table_from_prompt(prompt)
                tool_response = tool_function(table_name)
            elif "check query" in trigger_phrase:
                query = extract_query_from_prompt(prompt)
                tool_response = tool_function(query)

            # Incorporate the tool's result into the LLM's response
            llm_response += f"\n\nTool output:\n{tool_response}"
    
    return llm_response


def extract_query_from_prompt(prompt: str) -> str:
    # Logic to extract a SQL query from the prompt
    return "SELECT * FROM my_table LIMIT 10"  # Placeholder; add real extraction logic

def extract_table_from_prompt(prompt: str) -> str:
    # Logic to extract table name from the prompt
    return "my_table"  # Placeholder; add real extraction logic
