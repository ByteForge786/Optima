from cortex_inference import cortex_inference
import pandas as pd

def get_tools(con):
    def query_sql_database_tool(query: str):
        """Tool for querying Snowflake database."""
        try:
            results = pd.read_sql(query, con)
            query_id = con.cursor().sfqid
            return results, query_id
        except Exception as e:
            return f"Error: {e}", None

    def info_snowflake_table_tool(table_names: str):
        """Tool for getting metadata about Snowflake tables."""
        output_schema = ""
        _table_names = table_names.split(",")
        for t in _table_names:
            schema = pd.read_sql(f"DESCRIBE TABLE {t}", con)
            output_schema += f"Schema for table {t}: {schema}\n"
        return output_schema

    def query_sql_checker_tool(query: str):
        """Use Snowflake Cortex to check the SQL query for common mistakes."""
        prompt = f"""
        {query}
        Double check the query above for common mistakes, including:
        - Using NOT IN with NULL values
        - Using UNION when UNION ALL should have been used
        - Using BETWEEN for exclusive ranges
        - Data type mismatch in predicates
        - Properly quoting identifiers
        - Using the correct number of arguments for functions
        - Casting to the correct data type
        - Using the proper columns for joins
        If there are any mistakes, rewrite the query. Output the final SQL query only.
        """
        return cortex_inference(prompt)

    return {
        "query_sql_database_tool": query_sql_database_tool,
        "info_snowflake_table_tool": info_snowflake_table_tool,
        "query_sql_checker_tool": query_sql_checker_tool,
    }
