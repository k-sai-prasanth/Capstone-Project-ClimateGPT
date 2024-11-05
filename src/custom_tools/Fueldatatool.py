import os
import sys
from typing import Dict, Any, Optional
import pandas as pd
from commons.custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam

# Load the fuel dataset globally or within the class as needed
fuel_data = pd.read_csv('../Datasets/fuel_data.csv')

class FuelDataTool_Average(SingleMessageCustomTool):
    """
    Tool to get the average emission data for a given country and fuel type across all available years or a trend for
    the last/first x years. If no trend is specified, it returns the average by default. If a trend for a
    specific number of years is asked, it returns the data for the last or first x years, depending on the query.
    """

    def get_name(self) -> str:
        return "get_average_fuel_emission_data"

    def get_description(self) -> str:
        return "Get the average emission value or trend for a fuel type across all years or a specified range of years."

    def get_params_definition(self) -> Dict[str, ToolParamDefinitionParam]:
        return {
            "country": ToolParamDefinitionParam(
                param_type="str",
                description="The name of the country.",
                required=True,
            ),
            "fuel_type": ToolParamDefinitionParam(
                param_type="str",
                description="The type of fuel (e.g., 'Solid Fuel', 'Liquid Fuel', 'Gas Fuel', 'Cement', 'Gas Flaring').",
                required=True,
            ),
            "trend_type": ToolParamDefinitionParam(
                param_type="str",
                description="Whether to return 'average', 'last x years', 'first x years', or 'trend for x years' data.",
                required=False,  # Optional trend type parameter
            ),
            "num_years": ToolParamDefinitionParam(
                param_type="int",
                description="The number of years for which the trend is requested (only used with 'last x years', 'first x years', or 'trend for x years').",
                required=False,  # Optional parameter for specifying number of years
            ),
        }

    async def run_impl(self, country: str, fuel_type: str, trend_type: Optional[str] = "average", num_years: Optional[int] = 5, year: Optional[int] = None) -> Optional[Any]:
        # Convert country name to uppercase to match the dataset
        country = country.upper()
        
        # Filter the dataset for the specified country and year
        filtered_data = fuel_data[(fuel_data['Country'].str.upper() == country)]

        if year:
            filtered_data = filtered_data[filtered_data['Year'] == year]

        if filtered_data.empty:
            return {"error": f"No data available for the specified country: {country} in {year if year else 'all years'}."}

        # Check if the fuel_type exists in the dataset columns
        if fuel_type not in filtered_data.columns:
            return {"error": f'Fuel type "{fuel_type}" does not exist in the dataset.'}

        # If trend_type is 'average', calculate the average emission across all years
        if trend_type.lower() == "average":
            average_emission = filtered_data[fuel_type].mean()
            return {"country": country, "fuel_type": fuel_type, "average_emission": round(average_emission, 2)}

        # If trend_type contains 'last', get the trend for the last x years
        elif "last" in trend_type.lower() and 'Year' in filtered_data.columns:
            last_x_years_data = filtered_data.sort_values(by='Year', ascending=False).head(num_years)
            trend_last_x_years = last_x_years_data[['Year', fuel_type]].to_dict(orient='records')
            return {"country": country, "fuel_type": fuel_type, "trend_type": "last", "num_years": num_years, "data": trend_last_x_years}

        # If trend_type contains 'first', get the trend for the first x years
        elif "first" in trend_type.lower() and 'Year' in filtered_data.columns:
            first_x_years_data = filtered_data.sort_values(by='Year', ascending=True).head(num_years)
            trend_first_x_years = first_x_years_data[['Year', fuel_type]].to_dict(orient='records')
            return {"country": country, "fuel_type": fuel_type, "trend_type": "first", "num_years": num_years, "data": trend_first_x_years}

        # If trend_type contains 'trend for', get the trend for the most recent x years
        elif "trend for" in trend_type.lower() and 'Year' in filtered_data.columns:
            recent_x_years_data = filtered_data.sort_values(by='Year', ascending=False).head(num_years)
            trend_recent_x_years = recent_x_years_data[['Year', fuel_type]].to_dict(orient='records')
            return {"country": country, "fuel_type": fuel_type, "trend_type": "recent", "num_years": num_years, "data": trend_recent_x_years}

        # If trend_type is not recognized, return an error message
        else:
            return {"error": "Invalid trend type. Please choose 'average', 'last x years', 'first x years', or 'trend for x years'."}

