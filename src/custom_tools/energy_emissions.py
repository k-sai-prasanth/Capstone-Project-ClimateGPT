import os
import sys
from typing import Dict, Any, Optional, List, Union
import pandas as pd
from commons.custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam

# Load the emissions dataset globally or within the class as needed
# emissions_data = pd.read_csv('../Datasets/Energy_Emissions.csv')

class EnergyEmissionTool(SingleMessageCustomTool):
    """
    Tool to get energy emission data for a given country.
    Retrieve energy emission value for given country for a specified year for a given Series.
    Retrieve energy emission values for a specified country, specified series and specified year. 
    Retrieve energy emission values for a specified country, specified series if specified.
    Retrieve energy emission value for all countries for a specific year by retrieving data where Country/All Country == 'Total, all countries or areas' for a specific year.
    The function will return a JSON object with the requested information. 
    If multiple countries or multiple years are queried, the function will provide the corresponding outputs for each, delivering only the specific data requested.
    If the requested country or series type does not exist, it will return all series types for the given country and year.
    If 'all' is passed for country, year, or series, the function will include all available information for that parameter.
    """
    def __init__(self, data: pd.DataFrame = None):
        """Initialize the tool with an optional data parameter."""
        self.data = data if data is not None else pd.read_csv('../Datasets/Energy_Emissions.csv')

    def get_name(self) -> str:
        return "get_energy_emission_data"

    def get_description(self) -> str:
        return "Get Energy emission values for a given country for a specific year or for all years"

    def get_params_definition(self) -> Dict[str, ToolParamDefinitionParam]:
        return {
            "Country": ToolParamDefinitionParam(
                param_type="union",
                description="The name of the country or a list of countries, or 'all' to include all countries.",
                required=True,
            ),
            "Year": ToolParamDefinitionParam(
                param_type="union",
                description="A year or a list of years.",
                required=False,
            ),
            "Series": ToolParamDefinitionParam(
                param_type="union",
                description="The Series can include any information which are from 'Primary energy production (petajoules)','Net imports [Imports - Exports - Bunkers] (petajoules)','Total supply (petajoules)','Supply per capita (gigajoules)','Changes in stocks (petajoules)' or any information which are pertaining to the list.",
                required=False,
            ),
        }

    async def run_impl(self, Country: Union[str, List[str]], Year: Optional[Union[int, List[int]]] = None, Series: Optional[Union[str, List[str]]] = None) -> Optional[Any]:
        emissions_data = self.data.copy()
        # Step 1: Start with the full dataset
        filtered_data = emissions_data.copy()

        # Step 2: Apply Country filter only if Country is specified and not "all"
        if Country not in [None, "all"]:
            if isinstance(Country, str):
                Country = [Country]
            filtered_data = filtered_data[filtered_data['Country'].isin(Country)]

        # Step 3: Apply Year filter only if Year is specified and not "all"
        if Year not in [None, "all", []]:
            if isinstance(Year, int):
                Year = [Year]
            filtered_data = filtered_data[filtered_data['Year'].isin(Year)]

        # Step 4: Apply Series filter only if Series is specified and not "all"
        if Series not in [None, "all", []]:
            if isinstance(Series, str):
                Series = [Series]
            filtered_data = filtered_data[filtered_data['Series'].isin(Series)]

        # Check if data is available after filtering
        if filtered_data.empty:
            return {"message": "No data available for the specified filters."}

        # Convert 'Value' column to numeric, coercing errors to NaN
        filtered_data['Value'] = pd.to_numeric(filtered_data['Value'], errors='coerce')
        filtered_data = filtered_data.dropna(subset=['Value'])  # Remove rows with NaN in 'Value'

        # Step 5: Apply averaging logic if conditions are met
        if Country != "all" and (Year is None or Year == "all") and (Series is None or Series == "all"):
            average_data = (
                filtered_data.groupby(['Country', 'Series'])['Value']
                .mean()
                .reset_index()
                .rename(columns={'Value': 'Average Value'})
            )
            return average_data.to_json(orient="records", date_format="iso")

        # Default return without averaging
        result_data = filtered_data[['Country', 'Year', 'Series', 'Value']]
    
        return result_data.to_json(orient="records", date_format="iso")