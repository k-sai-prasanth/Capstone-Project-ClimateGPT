import os
import json
from typing import Optional, Any
import pandas as pd
from groq import Groq
from emissions_data import EmissionDataTool  # Specific year data
from emission_data_average import EmissionDataTool_Average  # Average data

# Set the Groq API key for the LLaMA model
GROQ_API_KEY = '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'

# Declaring the Groq API client
groq_client = Groq(api_key=GROQ_API_KEY)

# LLaMA Model Versions
llama_70B_tool_use = 'llama3-groq-70b-8192-tool-use-preview'

# Load the emissions dataset
emissions_data = pd.read_csv('../Datasets/GreenHouseEmissions.csv')

# Emission Query Handling Class
class EmissionQueryHandler:
    def __init__(self):
        self.tool_specific = EmissionDataTool()  # Tool for specific year data
        self.tool_average = EmissionDataTool_Average()  # Tool for average data

    # Function to handle both specific year and average queries
    async def fetch_emission_data(self, function_name: str, parameters: dict) -> Any:
        # Dynamically call the respective tool based on the function name
        if function_name == "get_emission_data":
            return await self.tool_specific.run_impl(**parameters)
        elif function_name == "get_average_emission_data":
            return await self.tool_average.run_impl(**parameters)
        else:
            return "Unknown function requested by the model."

# Tool declaration in the form of a JSON block
def get_tool_declaration():
    return """
    <tools> {
        "name": "get_emission_data",
        "description": "Get emission values for a given country, year, and emission type.",
        "parameters": {
            "properties": {
                "country": {
                    "description": "The name of the country or area.",
                    "type": "string"
                },
                "year": {
                    "description": "The year of the emission data.",
                    "type": "integer"
                },
                "emission_type": {
                    "description": "The type of emission (optional). E.g., 'sfc_emissions', 'n2o_emissions', 'HFC_PFC_emissions', 'nf3_emissions', 'pfc_emissions', 'methane_emissions', 'greenHouseGas_emissions_without_with_LandUse'.",
                    "type": "string"
                }
            },
            "required": ["country", "year"],
            "type": "object"
        }
    },
    {
        "name": "get_average_emission_data",
        "description": "Get the average emission value for a country across all years for a specified emission type.",
        "parameters": {
            "properties": {
                "country": {
                    "description": "The name of the country or area.",
                    "type": "string"
                },
                "emission_type": {
                    "description": "The type of emission (optional). E.g., 'sfc_emissions', 'n2o_emissions', 'HFC_PFC_emissions', 'nf3_emissions', 'pfc_emissions', 'methane_emissions', 'greenHouseGas_emissions_without_with_LandUse'.",
                    "type": "string"
                }
            },
            "required": ["country", "emission_type"],
            "type": "object"
        }
    } </tools>
    """

# Function to query the LLaMA model (Groq API) for the user's question and pass the tools
def query_llama_with_tools(question: str) -> str:
    tool_declaration = get_tool_declaration()
    
    chat_completion = groq_client.chat.completions.create(
        messages=[
            {
                "role": "system", 
                "content": (
                    f"You are a function-calling AI model. You have access to the following tools: {tool_declaration}. "
                    "For each function call, return a JSON object with the function name and the arguments required."
                )
            },
            {"role": "user", "content": question},
        ],
        model=llama_70B_tool_use,
    )
    return chat_completion.choices[0].message.content

# Function to parse the LLaMA model response and extract the function and parameters
def extract_function_and_parameters(response: str) -> Optional[dict]:
    print(f"Model Response: {response}")

    # Extract the JSON inside the <tool_call> tag
    try:
        tool_call_start = response.find("<tool_call>")
        tool_call_end = response.find("</tool_call>")
        if tool_call_start == -1 or tool_call_end == -1:
            print("No valid tool call found in response.")
            return None

        tool_call_json = response[tool_call_start + len("<tool_call>"):tool_call_end]
        tool_call_data = json.loads(tool_call_json)

        return {
            "function_name": tool_call_data.get("name"),
            "arguments": tool_call_data.get("arguments"),
        }
    except Exception as e:
        print(f"Error extracting tool call: {e}")
        return None

# Main function to handle the user input and call the correct emission tool
async def main():
    # Example question from the user
    question = "What is the average methane_emissions for Australia?"  # Example for specific year
    # question = "What is the average methane_emissions for Australia?"  # Example for average across all years
    # question = "What are the sfc_emissions for Canada in 2020?" # Example to get the respective information

    # Query the LLaMA model (Groq API)
    llama_response = query_llama_with_tools(question)

    # Extract the function name and parameters from the model's response
    function_and_parameters = extract_function_and_parameters(llama_response)

    if function_and_parameters:
        # Extract function name and parameters
        function_name = function_and_parameters.get("function_name")
        parameters = function_and_parameters.get("arguments")

        if function_name and parameters:
            # Call the appropriate emission tool using EmissionQueryHandler
            query_handler = EmissionQueryHandler()
            emission_data = await query_handler.fetch_emission_data(
                function_name=function_name,
                parameters=parameters
            )
            print(f"Emission Data: {emission_data}")
        else:
            print("Could not extract the function name or parameters.")
    else:
        print("Could not extract required parameters from the model's response.")

# Run the main function
if __name__ == "__main__":
    import asyncio
    asyncio.run(main())