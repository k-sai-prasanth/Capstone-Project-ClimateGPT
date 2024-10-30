import os
import sys
from typing import Dict, Any, Optional, List, Union
import pandas as pd
from custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam

# Load the emissions dataset globally or within the class as needed
emissions_data = pd.read_csv('../Datasets/Sector_Emissions.csv')

class SectorEmissionTool(SingleMessageCustomTool):
    """
    Tool to get emission data for a given country for a given sector.
    Retrieve emission value for given country for a specified year for a given sector for that description. Description has certain information such as energy, emission, cement emissions, EV market share, EV stock share, EVs per capita, Road Transport, coal, electricity, etc.
    Retrieve emission values for a specified country, specified sector and based on description. 
    Retrieve emission values for a specified country, specified sector by taking average of all year data of that sector and country. 
    The function will return a JSON object with the requested information. 
    If multiple countries, multiple sector types, or specific years are queried, the function will provide the corresponding outputs for each, delivering only the specific data requested.
    If the requested country or sector type does not exist, it will return all sector types for the given country and year.
    If 'all' is passed for country, year, or sector, the function will include all available information for that parameter.
    """

    def get_name(self) -> str:
        return "get_sector_emission_data"

    def get_description(self) -> str:
        return "Get sector emission values for a given sector and country"

    def get_params_definition(self) -> Dict[str, ToolParamDefinitionParam]:
        return {
            "Country": ToolParamDefinitionParam(
                param_type="union",
                description="The name of the country or a list of countries, or 'all' to include all countries.",
                required=True,
            ),
            "Sector": ToolParamDefinitionParam(
                param_type="union",
                description="The sector or a list of sectors or 'all' to include all sectors. Sectors can be Buildings, Industry, Electricity, Transport, Transport Road",
                required=False,
            ),
            "Year": ToolParamDefinitionParam(
                param_type="union",
                description="A year or a list of years.",
                required=False,
            ),
            "Description": ToolParamDefinitionParam(
                param_type="union",
                description="Description explaining if any specific type is required such as 'Buildings emissions intensity (per floor area, commercial)', 'Buildings emissions intensity (per floor area, residential)', 'Buildings energy intensity (commercial)', 'Buildings energy intensity (residential)', 'Emissions intensity of electricity generation', 'Share of coal in electricity generation', 'Cement emissions intensity (per product)', 'Steel emissions intensity (per product)', 'Zero emission fuels for domestic transport', 'EV market share','EV stock shares','EVs per capita','Road transport emissions intensity', 'Steel emissions intensity (per product)' or similar to the list in meaning. Use the terms in the list to pass to the function",
                required=False,
            ),
        }

    async def run_impl(self, Country: Union[str, List[str]], Sector: Optional[Union[str, List[str]]] = None, Year: Optional[Union[int, List[int]]] = None, Description: Optional[Union[str, List[str]]] = None) -> Optional[Any]:
        # Filter data based on the Country
        if Country == "all":
            filtered_data = emissions_data
        else:
            if isinstance(Country, str):
                Country = [Country]
            filtered_data = emissions_data[emissions_data['Country'].isin(Country)]

        # Filter by Sector if specified
        if Sector and Sector != "all":
            if isinstance(Sector, str):
                Sector = [Sector]
            filtered_data = filtered_data[filtered_data['Sector'].isin(Sector)]

        # Filter by Year if specified
        if Year and Year != "all":
            if isinstance(Year, int):
                Year = [Year]
            filtered_data = filtered_data[filtered_data['Year'].isin(Year)]

        # Filter by Description if specified
        if Description and Description != "all":
            if isinstance(Description, str):
                Description = [Description]
            filtered_data = filtered_data[filtered_data['Description'].isin(Description)]
    
        # Check conditions for averaging when Description is not specified
        if Country != "all" and (Year is None or Year == "all") and (Sector is None or Sector == "all") and (Description is None or Description == "all"):
            # Calculate the average emissions per sector for the specified country across all years
            average_data = (
                filtered_data.groupby(['Country', 'Sector'])['Emission Value']
                .mean()
                .reset_index()
                .rename(columns={'Emission Value': 'Average Emission Value'})
            )
            return average_data.to_json(orient="records", date_format="iso")
    
        # Check conditions for averaging when Description is specified
        elif Country != "all" and (Year is None or Year == "all") and (Sector is None or Sector == "all") and Description:
            # Calculate the average emissions per sector and description for the specified country across all years
            average_data = (
                filtered_data.groupby(['Country', 'Sector', 'Description'])['Emission Value']
                .mean()
                .reset_index()
                .rename(columns={'Emission Value': 'Average Emission Value'})
            )
            return average_data.to_json(orient="records", date_format="iso")

        # Default return for specific sectors/years/description with raw data
        return filtered_data.to_json(orient="records", date_format="iso")