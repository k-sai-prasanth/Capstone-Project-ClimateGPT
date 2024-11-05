import os
import pandas as pd
from typing import Dict, Any, Optional, List
from commons.custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam

# Load datasets for all 27 U.S. states
state_datasets = {
    "Alabama": pd.read_csv('../Datasets/Alabama_weather_data.csv'),
    "Arizona": pd.read_csv('../Datasets/Arizona_weather_data.csv'),
    "California": pd.read_csv('../Datasets/California_weather_data.csv'),
    "Colorado": pd.read_csv('../Datasets/Colorado_weather_data.csv'),
    "Florida": pd.read_csv('../Datasets/Florida_weather_data.csv'),
    "Georgia": pd.read_csv('../Datasets/Georgia_weather_data.csv'),
    "Illinois": pd.read_csv('../Datasets/Illinois_weather_data.csv'),
    "Indiana": pd.read_csv('../Datasets/Indiana_weather_data.csv'),
    "Kentucky": pd.read_csv('../Datasets/Kentucky_weather_data.csv'),
    "Louisiana": pd.read_csv('../Datasets/Louisiana_weather_data.csv'),
    "Maryland": pd.read_csv('../Datasets/Maryland_weather_data.csv'),
    "Massachusetts": pd.read_csv('../Datasets/Massachusetts_weather_data.csv'),
    "Michigan": pd.read_csv('../Datasets/Michigan_weather_data.csv'),
    "Minnesota": pd.read_csv('../Datasets/Minnesota_weather_data.csv'),
    "Missouri": pd.read_csv('../Datasets/Missouri_weather_data.csv'),
    "New York": pd.read_csv('../Datasets/New_York_weather_data.csv'),
    "New Jersey": pd.read_csv('../Datasets/New_Jersey_weather_data.csv'),
    "Pennsylvania": pd.read_csv('../Datasets/Pennsylvania_weather_data.csv'),
    "Texas": pd.read_csv('../Datasets/Texas_weather_data.csv'),
    "Virginia": pd.read_csv('../Datasets/Virginia_weather_data.csv'),
    "Washington": pd.read_csv('../Datasets/Washington_weather_data.csv'),
    # Add the remaining states as needed
}

class USStateWeatherDataTool(SingleMessageCustomTool):
    """
    Tool to get weather data for specified U.S. states, date range, and specific weather attributes.
    """

    def get_name(self) -> str:
        return "get_us_state_weather_data"

    def get_description(self) -> str:
        return "Retrieve weather data for specified attributes, date range, and state."

    def get_params_definition(self) -> Dict[str, ToolParamDefinitionParam]:
        return {
            "State": ToolParamDefinitionParam(
                param_type="str",
                description="The name of the U.S. state (e.g., 'California', 'Texas').",
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
                description="List of weather attributes to retrieve, e.g., 'tempmax', 'tempmin', 'temp', 'humidity', 'precip'.",
                required=False,
            ),
        }

    async def run_impl(self, State: str, StartDate: str, EndDate: str, Attributes: Optional[List[str]] = None) -> Optional[Any]:
        # Check if the state dataset exists
        if State not in state_datasets:
            return {"error": f"No data available for the specified state: {State}."}

        # Load the dataset for the specified state
        weather_data = state_datasets[State]
        
        # Verify 'datetime' column exists in the dataset
        if 'datetime' not in weather_data.columns:
            return {"error": f"The dataset for {State} does not contain a 'datetime' column."}
        
        # Convert 'datetime' column to datetime for filtering
        weather_data['datetime'] = pd.to_datetime(weather_data['datetime'], errors='coerce')
        weather_data.dropna(subset=['datetime'], inplace=True)  # Drop rows with invalid dates after conversion

        # Convert input dates to datetime objects
        try:
            start_date = pd.to_datetime(StartDate)
            end_date = pd.to_datetime(EndDate)
        except ValueError:
            return {"error": "Invalid date format. Use 'YYYY-MM-DD'."}

        # Ensure the specified date range is within the available data
        available_start = weather_data['datetime'].min()
        available_end = weather_data['datetime'].max()
        
        if start_date < available_start or end_date > available_end:
            return {
                "error": f"Data for {State} is only available from {available_start.strftime('%Y-%m-%d')} to {available_end.strftime('%Y-%m-%d')}."
            }

        # Ensure requested attributes exist in the dataset
        if Attributes:
            missing_attributes = [attr for attr in Attributes if attr not in weather_data.columns]
            if missing_attributes:
                return {"error": f"The following attributes are not available in the dataset for {State}: {', '.join(missing_attributes)}."}
        else:
            # Default attributes if none specified
            Attributes = ['tempmax', 'tempmin', 'temp', 'humidity', 'precip']

        # Filter data by date range
        filtered_data = weather_data[(weather_data['datetime'] >= start_date) & (weather_data['datetime'] <= end_date)]

        # Select only requested attributes along with the datetime
        result_data = filtered_data[['datetime'] + Attributes].dropna(subset=Attributes)

        # Check if data is available after filtering
        if result_data.empty:
            return {"error": "No data available for the specified date range and attributes."}

        # Convert result data to JSON format
        return {"status": "success", "data": result_data.to_dict(orient='records')}
