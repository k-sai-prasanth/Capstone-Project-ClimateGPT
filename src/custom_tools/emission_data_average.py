from typing import Dict, Any, Optional
import pandas as pd
from commons.custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import (
    ToolParamDefinitionParam,
)

# Load the emissions dataset globally or within the class as needed
# emissions_data = pd.read_csv('../Datasets/GreenHouseEmissions.csv')

class EmissionDataTool_Average(SingleMessageCustomTool):
    """
    Tool to get the average emission data for a given country across all available years or trend for the last/first x years.
    If no trend is specified, it returns the average by default. If a trend for a specific number of years is asked, it returns
    the data for the last or first x years, depending on the query.
    """
    def __init__(self, data: pd.DataFrame = None):
        """Initialize the tool with an optional data parameter."""
        self.data = data if data is not None else pd.read_csv('../Datasets/GreenHouseEmissions.csv')

    def get_name(self) -> str:
        return "get_average_emission_data"

    def get_description(self) -> str:
        return "Get the average emission value or trend for a country across all years for a specified emission type."

    def get_params_definition(self) -> Dict[str, ToolParamDefinitionParam]:
        return {
            "country": ToolParamDefinitionParam(
                param_type="str",
                description="The name of the country or area.",
                required=True,
            ),
            "emission_type": ToolParamDefinitionParam(
                param_type="str",
                description="The type of emission (optional). E.g., 'sfc_emissions', 'n2o_emissions', 'HFC_PFC_emissions', 'nf3_emissions', 'pfc_emissions', 'methane_emissions', 'green_house_emissions'.",
                required=True,  # Emission type is required for this calculation
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

    async def run_impl(self, country: str, emission_type: str, trend_type: Optional[str] = "average", num_years: Optional[int] = 5) -> Optional[Any]:
        emissions_data = self.data.copy()
        # Filter the dataset for the specified country
        filtered_data = emissions_data[emissions_data['Country or Area'] == country]

        if filtered_data.empty:
            return {"error": f"No data available for the specified country: {country}."}

        # Check if the emission_type exists in the dataset columns
        if emission_type not in filtered_data.columns:
            return {"error": f'Emission type "{emission_type}" does not exist.'}

        # If trend_type is 'average', calculate the average emission across all years
        if trend_type.lower() == "average":
            average_emission = filtered_data[emission_type].mean()
            return {"country": country, "emission_type": emission_type, "average_emission": round(average_emission, 2)}

        # If trend_type contains 'last', get the trend for the last x years
        elif "last" in trend_type.lower():
            last_x_years_data = filtered_data.sort_values(by='Year', ascending=False).head(num_years)
            trend_last_x_years = last_x_years_data[['Year', emission_type]].to_dict(orient='records')
            return {"country": country, "emission_type": emission_type, "trend_type": "last", "num_years": num_years, "data": trend_last_x_years}

        # If trend_type contains 'first', get the trend for the first x years
        elif "first" in trend_type.lower():
            first_x_years_data = filtered_data.sort_values(by='Year', ascending=True).head(num_years)
            trend_first_x_years = first_x_years_data[['Year', emission_type]].to_dict(orient='records')
            return {"country": country, "emission_type": emission_type, "trend_type": "first", "num_years": num_years, "data": trend_first_x_years}

        # If trend_type contains 'trend for', get the trend for the most recent x years
        elif "trend for" in trend_type.lower():
            recent_x_years_data = filtered_data.sort_values(by='Year', ascending=False).head(num_years)
            trend_recent_x_years = recent_x_years_data[['Year', emission_type]].to_dict(orient='records')
            return {"country": country, "emission_type": emission_type, "trend_type": "recent", "num_years": num_years, "data": trend_recent_x_years}

        # If trend_type is not recognized, return an error message
        else:
            return {"error": "Invalid trend type. Please choose 'average', 'last x years', 'first x years', or 'trend for x years'."}