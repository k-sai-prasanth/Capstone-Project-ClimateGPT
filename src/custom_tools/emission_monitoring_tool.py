import pandas as pd
from typing import Dict, Any
from commons.custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam

data = pd.read_csv('../Datasets/Emission_Monitoring_Data.csv')

class EmissionMonitoringTool(SingleMessageCustomTool):
    """
    This tool provides comprehensive access to greenhouse gas emission monitoring data, allowing users to analyze emissions
    by country, sector, and specific time periods. It supports filtering by emission type and offers insights into trends,
    enabling users to track changes in emissions over time.
    """

    def get_name(self) -> str:
        """
        Returns the unique identifier for this tool.

        Returns:
            str: The tool's name identifier.
        """
        return "get_emission_monitoring_data"

    def get_description(self) -> str:
        """
        Provides a detailed description of the tool's capabilities.

        Returns:
            str: A description of the tool's functionality.
        """
        return (
            "This tool enables detailed monitoring of greenhouse gas emissions data, allowing users to filter by country, "
            "sector, and emission type. It supports trend analysis and provides insights into changes in emissions across "
            "various sources, including industry, transport, and residential sectors."
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
                description="The country or list of countries to query (e.g., 'United States', 'China').",
                required=False,
            ),
            "sector": ToolParamDefinitionParam(
                param_type="str",
                description=(
                    "The sector to filter by. Options include:\n"
                    "- 'Industry'\n"
                    "- 'Transport'\n"
                    "- 'Residential'\n"
                    "- 'Agriculture'\n"
                    "- 'Energy'"
                ),
                required=False,
            ),
            "emission_type": ToolParamDefinitionParam(
                param_type="str",
                description=(
                    "The type of emission to analyze. Options include:\n"
                    "- 'CO2'\n"
                    "- 'Methane'\n"
                    "- 'N2O'\n"
                    "- 'HFCs'\n"
                    "- 'PFCs'\n"
                    "- 'SF6'"
                ),
                required=False,
            ),
            "start_year": ToolParamDefinitionParam(
                param_type="int",
                description="The starting year for the analysis (e.g., 2010).",
                required=False,
            ),
            "end_year": ToolParamDefinitionParam(
                param_type="int",
                description="The ending year for the analysis (e.g., 2023).",
                required=False,
            ),
        }

    async def run_impl(self, country: str = None, sector: str = None, emission_type: str = None, start_year: int = None, end_year: int = None) -> Dict[str, Any]:
        """
        Executes the analysis based on provided parameters, handling filtering by country, sector, emission type, and year range.

        Parameters:
            country (str, optional): The country to filter by. Defaults to None.
            sector (str, optional): The sector to filter by. Defaults to None.
            emission_type (str, optional): The emission type to analyze. Defaults to None.
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
                    return {"status": "error", "data": [f"No emission data found for the specified country: {country}."]}

            if sector:
                filtered_data = filtered_data[filtered_data['Sector'].str.contains(sector, case=False, na=False)]
                if filtered_data.empty:
                    return {"status": "error", "data": [f"No emission data found for the specified sector: {sector}."]}

            if emission_type:
                filtered_data = filtered_data[filtered_data['Emission Type'].str.contains(emission_type, case=False, na=False)]
                if filtered_data.empty:
                    return {"status": "error", "data": [f"No data found for the specified emission type: {emission_type}."]}

            if start_year and end_year:
                filtered_data = filtered_data[(filtered_data['Year'] >= start_year) & (filtered_data['Year'] <= end_year)]
                if filtered_data.empty:
                    return {"status": "error", "data": ["No data available for the specified year range."]}

            result = filtered_data[['Country', 'Sector', 'Year', 'Emission Type', 'Emissions']].to_dict(orient='records')

            if not result:
                return {"status": "error", "data": ["No results found based on the provided filters."]}

            return {"status": "success", "data": result}

        except Exception as e:
            return {"status": "error", "data": [str(e)]}
