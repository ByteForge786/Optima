import pandas as pd
from typing import List, Optional, Type, Sequence, Dict, Any, Union, Tuple
from snowflake.connector import SnowflakeConnection
from pydantic import BaseModel, Field

class _InfoSQLDatabaseToolInput(BaseModel):
    table_names: str = Field(
        ...,
        description=(
            "A comma-separated list of the table names for which to return the schema. "
            "Example input: 'table1, table2, table3'"
        ),
    )

class InfoSnowflakeTableTool:
    """Tool for getting metadata about Snowflake tables."""

    name: str = "sql_db_schema"
    description: str = "Get the schema and sample rows for the specified SQL tables."

    def __init__(self, conn_sf):
        self.conn_sf = conn_sf

    def run(self, table_names: str) -> str:
        """Get the schema for tables in a comma-separated list."""
        output_schema = ""
        _table_names = table_names.split(",")
        for t in _table_names:
            schema = pd.read_sql(f"DESCRIBE TABLE {t}", self.conn_sf)
            output_schema += f"Schema for table {t}:\n{schema.to_string()}\n\n"
        return output_schema

class _QuerySQLCheckerToolInput(BaseModel):
    query: str = Field(..., description="A detailed and SQL query to be checked.")

class QuerySQLCheckerTool:
    """Uses Snowflake Cortex to check if a query is correct."""

    name: str = "sql_db_query_checker"
    description: str = """
    Use this tool to double check if your query is correct before executing it.
    Always use this tool before executing a query with sql_db_query!
    """

    def __init__(self, conn_sf):
        self.conn_sf = conn_sf

    def run(self, query: str) -> str:
        """Use Cortex to check the query."""
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
        escaped_query = query.replace("'", "''")
        prompt = template.format(query=escaped_query)
        cortex_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'llama2-70b-chat',
            [
                {{
                    'role': 'system', 
                    'content': 'You are a helpful AI assistant that checks and optimizes Snowflake SQL queries.'
                }},
                {{
                    'role': 'user',
                    'content': '{prompt}'
                }}
            ],
            {{}}
        ) as response
        """
        result = pd.read_sql(cortex_query, self.conn_sf)
        return result['RESPONSE'].iloc[0]

class _QuerySQLDataBaseToolInput(BaseModel):
    query: str = Field(..., description="A detailed and correct SQL query.")

class QuerySQLDataBaseTool:
    """Tool for querying a Snowflake database."""

    name: str = "sql_db_query"
    description: str = """
    Execute a SQL query against the database and get back the result and query_id.
    If the query is not correct, an error message will be returned.
    If an error is returned, rewrite the query, check the query, and try again.
    """

    def __init__(self, conn_sf):
        self.conn_sf = conn_sf

    def run(self, query: str) -> Tuple[Union[str, pd.DataFrame], Optional[str]]:
        """Execute the query, return the results and query_id; or an error message."""
        try:
            cursor = self.conn_sf.cursor()
            cursor.execute(query)
            query_id = cursor.sfqid
            results = pd.read_sql(query, self.conn_sf)
            cursor.close()
            return results, query_id
        except Exception as e:
            return f"Error: {e}", None

class SnowflakeSQLOptimizer:
    def __init__(self, conn_sf):
        self.conn_sf = conn_sf
        self.info_tool = InfoSnowflakeTableTool(conn_sf)
        self.checker_tool = QuerySQLCheckerTool(conn_sf)
        self.query_tool = QuerySQLDataBaseTool(conn_sf)

    def run(self, input_query: str) -> str:
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

        cortex_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'llama2-70b-chat',
            [
                {{
                    'role': 'system', 
                    'content': '{system_message}'
                }},
                {{
                    'role': 'user',
                    'content': '{input_query}'
                }}
            ],
            {{}}
        ) as response
        """

        result = pd.read_sql(cortex_query, self.conn_sf)
        assistant_response = result['RESPONSE'].iloc[0]

        # Process the assistant's response
        steps = assistant_response.split('\n')
        for step in steps:
            if step.startswith("1. Identify Expensive Queries"):
                query = """
                SELECT query_text, execution_time, bytes_scanned
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
                  AND query_type = 'SELECT'
                ORDER BY execution_time DESC
                LIMIT 20
                """
                expensive_queries, _ = self.query_tool.run(query)
                print("Expensive queries identified.")

            elif step.startswith("2. Analyze Query Structure"):
                for _, row in expensive_queries.iterrows():
                    query_text = row['QUERY_TEXT']
                    tables = self._extract_tables(query_text)
                    for table in tables:
                        schema = self.info_tool.run(table)
                        print(f"Schema for table {table}:")
                        print(schema)

            elif step.startswith("3. Suggest Optimizations"):
                optimizations = self._suggest_optimizations(expensive_queries)
                print("Optimization suggestions:")
                print(optimizations)

            elif step.startswith("4. Validate Improvements"):
                for original_query, optimized_query in optimizations:
                    original_result, original_query_id = self.query_tool.run(original_query)
                    optimized_result, optimized_query_id = self.query_tool.run(optimized_query)
                    
                    if original_result.equals(optimized_result):
                        print("Results match. Comparing performance...")
                        self._compare_performance(original_query_id, optimized_query_id)
                    else:
                        print("Results do not match. Optimization may be incorrect.")

            elif step.startswith("5. Prepare Summary"):
                summary = self._prepare_summary()
                print("Summary:")
                print(summary)

        return "Optimization process completed."

    def _extract_tables(self, query):
        # This is a simplified example. In a real scenario, you'd want to use a proper SQL parser.
        words = query.split()
        tables = []
        for i, word in enumerate(words):
            if word.upper() == "FROM" or word.upper() == "JOIN":
                if i + 1 < len(words):
                    tables.append(words[i + 1])
        return list(set(tables))

    def _suggest_optimizations(self, expensive_queries):
        optimizations = []
        for _, row in expensive_queries.iterrows():
            original_query = row['QUERY_TEXT']
            optimization_prompt = f"Suggest optimizations for this query:\n{original_query}"
            optimized_query = self.checker_tool.run(optimization_prompt)
            optimizations.append((original_query, optimized_query))
        return optimizations

    def _compare_performance(self, original_query_id, optimized_query_id):
        query = f"""
        SELECT query_id, execution_time, bytes_scanned
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE query_id IN ('{original_query_id}', '{optimized_query_id}')
        """
        performance_data, _ = self.query_tool.run(query)
        print("Performance comparison:")
        print(performance_data)

    def _prepare_summary(self):
        # In a real scenario, you'd want to aggregate the results from all previous steps
        return "Summary of optimization process..."

# Example usage
def create_snowflake_sql_optimizer(conn_sf):
    return SnowflakeSQLOptimizer(conn_sf)

# To use the optimizer:
# 1. Set up your Snowflake connection
# 2. Create the optimizer:
#    optimizer = create_snowflake_sql_optimizer(conn_sf)
# 3. Use the optimizer:
#    result = optimizer.run("Optimize the most expensive queries from the last 7 days")
