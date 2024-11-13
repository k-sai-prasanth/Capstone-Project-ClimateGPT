import pandas as pd
from typing import Dict, Any
from commons.custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam

data = pd.read_csv('../Datasets/Energy_Mix_Data.csv')

class EnergyMixDataTool(SingleMessageCustomTool):
    """
    This tool provides access to the energy mix data of various countries, enabling analysis of the contributions
    of different energy sources such as coal, renewable, nuclear, and natural gas. It supports filtering by country,
    year range, and energy source type, allowing users to gain insights into energy trends and transitions globally.
    """

    def get_name(self) -> str:
        """
        Returns the unique identifier for this tool.

        Returns:
            str: The tool's name identifier.
        """
        return "get_energy_mix_data"

    def get_description(self) -> str:
        """
        Provides a detailed description of the tool's capabilities.

        Returns:
            str: A description of the tool's functionality.
        """
        return (
            "This tool allows users to query energy mix data by country, year range, and energy source type, providing insights "
            "into the contributions of various energy sources such as coal, renewable, nuclear, and natural gas. It supports trend "
            "analysis and helps in understanding the energy transitions in different countries."
        )

    def get_params_definition(self) -> Dict[str, ToolParamDefinitionParam]:
        """
        Defines the parameters for querying the dataset.

        Returns:
            Dict[str, ToolParamDefinitionParam]: The parameters and their specifications.
        """
        return {
            "country": ToolParamDefinitionParam(
                param_type="str",
                description="The country or list of countries to query (e.g., 'Germany', 'China').",
                required=False,
            ),
            "energy_source": ToolParamDefinitionParam(
                param_type="str",
                description=(
                    "The type of energy source to filter by. Options include:\n"
                    "- 'Coal'\n"
                    "- 'Renewable'\n"
                    "- 'Nuclear'\n"
                    "- 'Natural Gas'\n"
                    "- 'Hydro'\n"
                    "- 'Solar'\n"
                    "- 'Wind'"
                ),
                required=False,
            ),
            "start_year": ToolParamDefinitionParam(
                param_type="int",
                description="The starting year for the analysis (e.g., 2000).",
                required=False,
            ),
            "end_year": ToolParamDefinitionParam(
                param_type="int",
                description="The ending year for the analysis (e.g., 2023).",
                required=False,
            ),
        }

    async def run_impl(self, country: str = None, energy_source: str = None, start_year: int = None, end_year: int = None) -> Dict[str, Any]:
        """
        Executes the analysis based on provided parameters, handling filtering by country, energy source, and year range.

        Parameters:
            country (str, optional): The country or list of countries to filter by. Defaults to None.
            energy_source (str, optional): The energy source type to filter by. Defaults to None.
            start_year (int, optional): The starting year for the analysis. Defaults to None.
            end_year (int, optional): The ending year for the analysis. Defaults to None.

        Returns:
            Dict[str, Any]: The analysis results or error messages if no data is found.
        """
        try:
            filtered_data = data.copy()

            if country:
                filtered_data = filtered_data[filtered_data['Country'].str.contains(country, case=False, na=False)]
                if filtered_data.empty:
                    return {"status": "error", "data": [f"No energy mix data found for the specified country: {country}."]}

            if energy_source:
                filtered_data = filtered_data[filtered_data['Energy Source'].str.contains(energy_source, case=False, na=False)]
                if filtered_data.empty:
                    return {"status": "error", "data": [f"No data available for the specified energy source: {energy_source}."]}

            if start_year and end_year:
                filtered_data = filtered_data[(filtered_data['Year'] >= start_year) & (filtered_data['Year'] <= end_year)]
                if filtered_data.empty:
                    return {"status": "error", "data": ["No data available for the specified year range."]}

            result = filtered_data[['Country', 'Year', 'Energy Source', 'Percentage']].to_dict(orient='records')

            if not result:
                return {"status": "error", "data": ["No results found based on the provided filters."]}

            return {"status": "success", "data": result}

        except Exception as e:
            return {"status": "error", "data": [str(e)]}
