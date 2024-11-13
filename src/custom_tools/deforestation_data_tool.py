import pandas as pd
from typing import Dict, Any
from commons.custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam

data = pd.read_csv('../Datasets/Deforestation_Data.csv')

class DeforestationDataTool(SingleMessageCustomTool):
    """
    This tool provides access to global deforestation data, enabling analysis of deforestation rates across different countries
    and forest types. It supports filtering by country, forest type, and year range, allowing users to gain insights into
    the extent and impact of deforestation globally.
    """

    def get_name(self) -> str:
        """
        Returns the unique identifier for the tool.

        Returns:
            str: The tool's name identifier.
        """
        return "get_deforestation_data"

    def get_description(self) -> str:
        """
        Provides a detailed description of the tool's capabilities.

        Returns:
            str: A description of the tool's functionality.
        """
        return (
            "This tool enables detailed analysis of global deforestation data, including deforestation rates, forest area changes, "
            "and the impact of land use changes. Users can filter by country, forest type, and year range to gain insights into "
            "the trends and drivers of deforestation."
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
                description="The country or list of countries to query (e.g., 'Brazil', 'Indonesia').",
                required=False,
            ),
            "forest_type": ToolParamDefinitionParam(
                param_type="str",
                description=(
                    "The type of forest to analyze. Options include:\n"
                    "- 'Tropical'\n"
                    "- 'Temperate'\n"
                    "- 'Boreal'\n"
                    "- 'Mangrove'"
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

    async def run_impl(self, country: str = None, forest_type: str = None, start_year: int = None, end_year: int = None) -> Dict[str, Any]:
        """
        Executes the analysis based on provided parameters, handling filtering by country, forest type, and year range.

        Parameters:
            country (str, optional): The country or list of countries to filter by. Defaults to None.
            forest_type (str, optional): The type of forest to analyze. Defaults to None.
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
                    return {"status": "error", "data": [f"No deforestation data found for the specified country: {country}."]}

            if forest_type:
                filtered_data = filtered_data[filtered_data['Forest Type'].str.contains(forest_type, case=False, na=False)]
                if filtered_data.empty:
                    return {"status": "error", "data": [f"No data available for the specified forest type: {forest_type}."]}

            if start_year and end_year:
                filtered_data = filtered_data[(filtered_data['Year'] >= start_year) & (filtered_data['Year'] <= end_year)]
                if filtered_data.empty:
                    return {"status": "error", "data": ["No data available for the specified year range."]}

            result = filtered_data[['Country', 'Forest Type', 'Year', 'Deforestation Rate']].to_dict(orient='records')

            if not result:
                return {"status": "error", "data": ["No results found based on the provided filters."]}

            return {"status": "success", "data": result}

        except Exception as e:
            return {"status": "error", "data": [str(e)]}
