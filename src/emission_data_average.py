import os
import sys
from typing import Dict, Any, Optional
import pandas as pd
from custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import (
    ToolParamDefinitionParam,
)

# Load the emissions dataset globally or within the class as needed
emissions_data = pd.read_csv('../Datasets/GreenHouseEmissions.csv')

class EmissionDataTool_Average(SingleMessageCustomTool):
    """Tool to get the average emission data for a given country across all available years."""

    def get_name(self) -> str:
        return "get_average_emission_data"

    def get_description(self) -> str:
        return "Get the average emission value for a country across all years for a specified emission type."

    def get_params_definition(self) -> Dict[str, ToolParamDefinitionParam]:
        return {
            "country": ToolParamDefinitionParam(
                param_type="str",
                description="The name of the country or area.",
                required=True,
            ),
            "emission_type": ToolParamDefinitionParam(
                param_type="str",
                description="The type of emission (optional). E.g., 'sfc_emissions', 'n2o_emissions', 'HFC_PFC_emissions', 'nf3_emissions', 'pfc_emissions', 'methane_emissions', 'greenHouseGas_emissions_without_with_LandUse'.",
                required=True,  # Emission type is required for this average calculation
            ),
        }

    async def run_impl(self, country: str, emission_type: str) -> Optional[Any]:
        # Filter the dataset for the specified country
        filtered_data = emissions_data[emissions_data['Country or Area'] == country]

        if filtered_data.empty:
            return f"No data available for the specified country: {country}."

        # Check if the emission_type exists in the dataset columns
        if emission_type not in filtered_data.columns:
            return f'Emission type "{emission_type}" does not exist.'

        # Calculate the average emission for the specified country and emission type
        average_emission = filtered_data[emission_type].mean()

        # Return the average rounded to 2 decimal places
        return round(average_emission, 2)