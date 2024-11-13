import pandas as pd
from typing import Dict, Any
from commons.custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam

data = pd.read_csv('../Datasets/Global_Carbon_Emissions.csv')

class GlobalCarbonEmissionsTool(SingleMessageCustomTool):
    """
    This tool provides access to global carbon emissions data, allowing users to query emissions by country, sector, year,
    and emission type. It supports detailed filtering options and offers insights into trends and changes in carbon emissions
    across different countries and sectors.
    """

    def get_name(self) -> str:
        """
        Returns the unique identifier for this tool.

        Returns:
            str: The tool's name identifier.
        """
        return "get_global_carbon_emissions"

    def get_description(self) -> str:
        """
        Provides a detailed description of the tool's capabilities.

        Returns:
            str: A description of the tool's functionality.
        """
        return (
            "This tool enables detailed analysis of global carbon emissions data, allowing users to query emissions by country, "
            "sector, year, and emission type. It supports filtering options for trend analysis and insights into changes "
            "in emissions across various countries and sectors."
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
                description="The country or list of countries to query (e.g., 'India', 'United States').",
                required=False,
            ),
            "sector": ToolParamDefinitionParam(
                param_type="str",
                description=(
                    "The sector for which to retrieve emission data. Options include:\n"
                    "- 'Industry'\n"
                    "- 'Transport'\n"
                    "- 'Residential'\n"
                    "- 'Energy'\n"
                    "- 'Agriculture'"
                ),
                required=False,
            ),
            "year": ToolParamDefinitionParam(
                param_type="int",
                description="The specific year for which to retrieve emission data (e.g., 2020).",
                required=False,
            ),
            "emission_type": ToolParamDefinitionParam(
                param_type="str",
                description=(
                    "The type of emission to query. Options include:\n"
                    "- 'CO2'\n"
                    "- 'Methane'\n"
                    "- 'N2O'\n"
                    "- 'HFCs'\n"
                    "- 'PFCs'"
                ),
                required=False,
            ),
        }

    async def run_impl(self, country: str = None, sector: str = None, year: int = None, emission_type: str = None) -> Dict[str, Any]:
        """
        Executes the analysis based on provided parameters, handling filtering by country, sector, year, and emission type.

        Parameters:
            country (str, optional): The country or list of countries to filter by. Defaults to None.
            sector (str, optional): The sector to filter by. Defaults to None.
            year (int, optional): The year to filter by. Defaults to None.
            emission_type (str, optional): The emission type to query. Defaults to None.

        Returns:
            Dict[str, Any]: The analysis results or error messages if no data is found.
        """
        try:
            filtered_data = data.copy()

            if country:
                filtered_data = filtered_data[filtered_data['Country'].str.contains(country, case=False, na=False)]
                if filtered_data.empty:
                    return {"status": "error", "data": [f"No emission data found for the specified country: {country}."]}

            if sector:
                filtered_data = filtered_data[filtered_data['Sector'].str.contains(sector, case=False, na=False)]
                if filtered_data.empty:
                    return {"status": "error", "data": [f"No emission data found for the specified sector: {sector}."]}

            if year:
                filtered_data = filtered_data[filtered_data['Year'] == year]
                if filtered_data.empty:
                    return {"status": "error", "data": [f"No emission data found for the specified year: {year}."]}

            if emission_type:
                filtered_data = filtered_data[filtered_data['Emission Type'].str.contains(emission_type, case=False, na=False)]
                if filtered_data.empty:
                    return {"status": "error", "data": [f"No data found for the specified emission type: {emission_type}."]}

            result = filtered_data[['Country', 'Sector', 'Year', 'Emission Type', 'Emissions']].to_dict(orient='records')

            if not result:
                return {"status": "error", "data": ["No results found based on the provided filters."]}

            return {"status": "success", "data": result}

        except Exception as e:
            return {"status": "error", "data": [str(e)]}
