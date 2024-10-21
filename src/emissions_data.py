
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

class EmissionDataTool(SingleMessageCustomTool):
    """Tool to get emission data for a given country and year."""

    def get_name(self) -> str:
        return "get_emission_data"

    def get_description(self) -> str:
        return "Get emission values for a given country, year, and emission type."

    def get_params_definition(self) -> Dict[str, ToolParamDefinitionParam]:
        return {
            "country": ToolParamDefinitionParam(
                param_type="str",
                description="The name of the country or area.",
                required=True,
            ),
            "year": ToolParamDefinitionParam(
                param_type="int",
                description="The year of the emission data.",
                required=True,
            ),
            "emission_type": ToolParamDefinitionParam(
                param_type="str",
                description="The type of emission (optional). E.g., 'sfc_emissions', 'n2o_emissions, green_house_emissions'.",
                required=False,
            ),
        }

    async def run_impl(self, country: str, year: int, emission_type: Optional[str] = None) -> Optional[Any]:
        # Filter the dataset for the specified country and year
        filtered_data = emissions_data[(emissions_data['Country or Area'] == country) & (emissions_data['Year'] == year)]

        if filtered_data.empty:
            return "No data available for the specified country and year."

        if emission_type:
            if emission_type in filtered_data.columns:
                return filtered_data[emission_type].values[0]
            else:
                return f'Emission type "{emission_type}" does not exist.'
        
        return filtered_data.to_json(orient="records", date_format="iso")