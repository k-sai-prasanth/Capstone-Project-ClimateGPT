import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from typing import Dict, Any, Optional, List, Union
from commons.custom_tools import SingleMessageCustomTool  # Import after modifying sys.path
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam

# Load the dataset globally or within the class as needed
weather_data = pd.read_csv('../Datasets/United Kingdom 2023-01-01 to 2024-11-19.csv')

class UK23WeatherDataTool(SingleMessageCustomTool):
    """
    Tool to get weather data for a specified country, date range, and specific weather attributes.
    This tool can provide information on temperature, precipitation, wind, humidity, solar data, 
    and various other metrics over a specific date range.
    Supports querying by country, date range, and weather attributes, and returns JSON-formatted results.
    """

    def get_name(self) -> str:
        return "get_weather_data"

    def get_description(self) -> str:
        return "Retrieve weather data for specified attributes, date ranges, and countries."

    def get_params_definition(self) -> Dict[str, ToolParamDefinitionParam]:
        return {
            "Country": ToolParamDefinitionParam(
                param_type="str",
                description="The name of the country. Use 'all' to include all available countries.",
                required=True,
            ),
            "StartDate": ToolParamDefinitionParam(
                param_type="str",
                description="The start date for the range in 'YYYY-MM-DD' format.",
                required=True,
            ),
            "EndDate": ToolParamDefinitionParam(
                param_type="str",
                description="The end date for the range in 'YYYY-MM-DD' format.",
                required=True,
            ),
            "Attributes": ToolParamDefinitionParam(
                param_type="list",
                description="List of attributes to retrieve, e.g., 'tempmax', 'tempmin', 'humidity', 'precip', 'windspeed'.",
                required=False,
            ),
        }

    async def run_impl(self, Country: str, StartDate: str, EndDate: str, Attributes: Optional[List[str]] = None) -> Optional[Any]:
        # Step 1: Filter by Country if specified and not "all"
        filtered_data = weather_data.copy()
        if Country.lower() != "all":
            filtered_data = filtered_data[filtered_data['name'] == Country]
        
        # Step 2: Filter by Date Range
        filtered_data['datetime'] = pd.to_datetime(filtered_data['datetime'])
        filtered_data = filtered_data[(filtered_data['datetime'] >= StartDate) & (filtered_data['datetime'] <= EndDate)]

        # Step 3: Check if data exists after filtering
        if filtered_data.empty:
            return {"message": "No data available for the specified filters."}
        
        # Step 4: Filter columns based on requested attributes
        available_attributes = set(weather_data.columns)
        if Attributes:
            requested_attributes = set(Attributes)
            invalid_attributes = requested_attributes - available_attributes
            if invalid_attributes:
                return {"error": f"Invalid attributes requested: {', '.join(invalid_attributes)}"}
            filtered_data = filtered_data[['datetime', 'name'] + list(requested_attributes)]
        else:
            # Default to essential columns if no specific attributes are requested
            filtered_data = filtered_data[['datetime', 'name', 'tempmax', 'tempmin', 'humidity', 'precip', 'windspeed', 'conditions']]

        # Step 5: Return data in JSON format
        result_data = filtered_data.to_json(orient="records", date_format="iso")
        return result_data
