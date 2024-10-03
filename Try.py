import snowflake.connector
import pandas as pd
from typing import Dict, Any, List
from langchain.llms import BaseLLM
from langchain.agents import ZeroShotAgent
from langchain.prompts import PromptTemplate

def cortex_inference(prompt: str) -> str:
    # Use Snowflake Cortex for inference, calling the SNOWFLAKE.CORTEX.COMPLETE function
    query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', '{prompt}');"
    
    # Assuming you have a working Snowflake connection
    con = snowflake.connector.connect(
        user='your_user',
        password='your_password',
        account='your_account',
        warehouse='your_warehouse',
        database='your_database',
        schema='your_schema'
    )
    
    result = pd.read_sql(query, con)
    con.close()
    return result.iloc[0, 0]

# Create LLM wrapper
class SnowflakeCortexLLM(BaseLLM):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def generate(self, prompts: List[str]) -> List[str]:
        # Call cortex_inference for each prompt and return the results
        return [cortex_inference(prompt) for prompt in prompts]

    @property
    def _identifying_params(self) -> Dict[str, Any]:
        return {}

    def _create_output_parser(self):
        return None

# Define a prompt template
prompt_template = PromptTemplate.from_template("What is the capital of {country}?")

# Create an instance of your Snowflake Cortex LLM
cortex_llm = SnowflakeCortexLLM()

# Create the agent
agent = ZeroShotAgent(llm=cortex_llm, prompt_template=prompt_template)

# Use the agent
country = "France"
response = agent.run(country=country)
print(response)
