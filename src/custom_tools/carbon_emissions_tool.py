import pandas as pd
from typing import Dict, Any, List
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam
from commons.custom_tools import SingleMessageCustomTool


class CarbonEmissionDataTool(SingleMessageCustomTool):
    """
    Tool to retrieve CO2 emission data based on specified criteria, including countries, sectors, years, and dates.
    
    """
    def __init__(self, data: pd.DataFrame = None):
        """Initialize the tool with an optional data parameter."""
        self.data = data if data is not None else pd.read_csv('../Datasets/carbon_monitor_global.csv')

    def get_name(self) -> str:
        """Returns the name of the tool used for invocation."""
        return "get_carbon_emission_data"

    def get_description(self) -> str:
        """
        Tool Description

        This tool allows the user to query CO2 emission data for one or more countries,
        sectors, and dates or years. It supports multiple operations such as filtering,
        aggregation, and error handling for missing data.
        """
        return (
            "Query CO2 emissions data by specifying countries, sectors, years, and/or dates. "
            "If any of the requested parameters are not available, it provides relevant messages."
            "It has information for every day from January to July for the years 2023 and 2024."
            "The information is specified for the following countries only: Brazil, China, European Union, France, Germany, India, Italy, Japan, Russia, Spain, United Kingdom, United States, Rest of the World and WORLD."
            "It has the data for the following sectors, Domestic Aviation, Ground Transport, Industry, Residential, Power and ,International Aviation"
        )

    def get_parameters(self) -> Dict[str, ToolParamDefinitionParam]:
        """Defines the parameters for the tool."""
        return {
            "countries": ToolParamDefinitionParam(
                param_type = "str",
                description =(
                    "The country or list of countries for which to fetch the co2 or carbon emissions data. "
                    "The information is specified for the following countries only: Brazil, China, European Union, France, Germany, India, Italy, Japan, Russia, Spain, United Kingdom, United States, Rest of the World and WORLD."
                    "For example, 'India' or 'India, Brazil'. If not provided, data for all countries will be aggregated."
                ),
                required=False,
            ),
            "sectors": ToolParamDefinitionParam(
                param_type = "str",
                description =(
                    "The sector or list of sectors for which to fetch the co2 or carbon emissions data. "
                    "It has the data for the following sectors, Domestic Aviation, Ground Transport, Industry, Residential, Power and ,International Aviation"
                    "For example, 'Residential' or 'Residential, Power'. If not provided, data for all sectors will be aggregated."
                ),
                required=False,
            ),
            "years": ToolParamDefinitionParam(
                param_type = "int",
                description =(
                    "The specific year or list of years for which to filter the co2 or carbon emissions data. "
                    "It has information for every day from January to July for the years 2023 and 2024."
                    "For example, '2023' or '2023, 2024'."
                ),
                required=False,
            ),
            "dates": ToolParamDefinitionParam(
                param_type = "str",
                description =(
                    "The specific date for which to filter the co2 or carbon emissions data. "
                    " The data is in the format of DD/MM/YYYY "
                    "It has information for every day from January to July for the years 2023 and 2024."
                    "For example, '01/01/2023'. "
                ),
                required=False,
            ),
        }

    async def run_impl(self, countries: List[str] = None, sectors: List[str] = None, years: List[int] = None, dates: List[str] = None) -> Dict[str, Any]:
        """
        Filters the emissions data based on the specified countries, sectors, years, and/or dates.
        Returns a JSON-like dictionary containing the filtered data and error messages if applicable.
        
        Parameters:
        - countries (list): List of countries to filter by (optional)
        - sectors (list): List of sectors to filter by (optional)
        - years (list): List of years to filter by (optional)
        - dates (list): List of specific dates to filter by in 'DD/MM/YYYY' format (optional)
        
        Returns:
        - dict: A dictionary with the filtered emission data and any error messages
        """
        filtered_data = self.data.copy()
        messages = []

        # Filtering by countries
        if countries:
            available_countries = filtered_data['country'].unique()
            missing_countries = [c for c in countries if c not in available_countries]
            if missing_countries:
                messages.append(f"Data for the following countries isn't available: {', '.join(missing_countries)}")
            filtered_data = filtered_data[filtered_data['country'].isin(countries)]
        
        # Filtering by sectors
        if sectors:
            available_sectors = filtered_data['sector'].unique()
            missing_sectors = [s for s in sectors if s not in available_sectors]
            if missing_sectors:
                messages.append(f"Data for the following sectors in these countries isn't available: {', '.join(missing_sectors)}")
            filtered_data = filtered_data[filtered_data['sector'].isin(sectors)]
        
        # Filtering by dates or years
        if dates:
            filtered_data = filtered_data[filtered_data['date'].isin(dates)]
            emission_summary = filtered_data.groupby(['country', 'sector'])['MtCO2 per day'].sum().reset_index()
        elif years:
            filtered_data['year'] = pd.to_datetime(filtered_data['date'], format='%d/%m/%Y').dt.year
            filtered_data = filtered_data[filtered_data['year'].isin(years)]
            emission_summary = filtered_data.groupby(['country', 'sector'])['MtCO2 per day'].sum().reset_index()
        else:
            emission_summary = filtered_data.groupby(['country', 'sector'])['MtCO2 per day'].mean().reset_index()

        # Renaming columns for final output
        emission_summary.columns = ['Country', 'Sector', 'Emissions']
        result = emission_summary.to_dict(orient='records')

        # IFinal Output
        output = {
            'data': result,
            'messages': messages
        }
        return output
