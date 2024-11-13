import pandas as pd
from typing import Dict, Any
from commons.custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam

data = pd.read_csv('../Datasets/Global_Surface_Water.csv')

class SurfaceWaterDataTool(SingleMessageCustomTool):
    """
    This tool enables querying of global surface water data, offering insights into water occurrence, changes, and seasonal trends.
    It supports a wide range of parameters for detailed filtering, allowing users to analyze specific regions and time periods.
    The dataset focuses on global regions and their surface water characteristics over several years.
    """

    def get_name(self) -> str:
        """
        Provides the unique name identifier for this tool. This name is utilized when invoking function calls related to surface water data.
        
        Returns:
            str: The name identifier of the tool.
        """
        return "get_surface_water_data"

    def get_description(self) -> str:
        """
        Describes the capabilities of the tool, explaining what kind of data can be queried and analyzed.
        
        This tool allows for comprehensive analysis of surface water data across various global regions. It includes options
        to analyze the presence of water (occurrence), changes over specified periods, and seasonal variations.
        Users can specify regions and timeframes, making it versatile for environmental monitoring and research.
        
        Returns:
            str: A comprehensive description of the tool's purpose and functionality.
        """
        return (
            "This tool provides access to global surface water data, enabling detailed analysis of water presence, changes over time, "
            "and seasonal patterns. Users can filter by region, specific years, and choose different analysis types. "
            "The tool supports insights into water occurrence, temporal changes, and seasonal trends."
        )

    def get_params_definition(self) -> Dict[str, ToolParamDefinitionParam]:
        """
        Defines the parameters available for querying the dataset, including their types and descriptions.
        
        The parameters cover region selection, year range, and the type of analysis to perform. These parameters guide the tool's behavior,
        determining what data to fetch and how to interpret it.
        
        Returns:
            Dict[str, ToolParamDefinitionParam]: A dictionary outlining the available parameters and their specifications.
        """
        return {
            "region": ToolParamDefinitionParam(
                param_type="str",
                description=(
                    "The region or geographical area to analyze for surface water data. Examples: 'Amazon Basin', 'Great Lakes', "
                    "'Australian Outback'. If not specified, the tool will provide a global overview."
                ),
                required=False,
            ),
            "start_year": ToolParamDefinitionParam(
                param_type="int",
                description=(
                    "The initial year for the analysis. If not specified, the dataset's full time span will be included. "
                    "Useful for observing changes over a specific period."
                ),
                required=False,
            ),
            "end_year": ToolParamDefinitionParam(
                param_type="int",
                description=(
                    "The final year for the analysis. If not specified, all available years up to the latest will be considered. "
                    "Use in combination with the start year for time-specific analysis."
                ),
                required=False,
            ),
            "analysis_type": ToolParamDefinitionParam(
                param_type="str",
                description=(
                    "The type of analysis to perform. Options include:\n"
                    "- 'occurrence': Analyze overall water presence in the selected region.\n"
                    "- 'change': Detect changes in water presence over the specified time period.\n"
                    "- 'seasonality': Identify seasonal variations in water presence."
                ),
                required=False,
            ),
        }

    async def run_impl(self, region: str = None, start_year: int = None, end_year: int = None, analysis_type: str = "occurrence") -> Dict[str, Any]:
        """
        Executes the analysis based on the parameters provided, filtering the dataset accordingly.
        Handles region-specific queries, time-based filtering, and different types of surface water analysis.

        Parameters:
            region (str, optional): The geographical region to focus on for the analysis. Defaults to None.
            start_year (int, optional): The beginning year for filtering data. Defaults to None.
            end_year (int, optional): The ending year for filtering data. Defaults to None.
            analysis_type (str, optional): The specific analysis type requested (e.g., 'occurrence', 'change', 'seasonality'). Defaults to 'occurrence'.

        Returns:
            Dict[str, Any]: A structured dictionary containing the analysis results or error messages if issues occur.
        """
        try:
            filtered_data = data.copy()

            if region:
                filtered_data = filtered_data[filtered_data['Region'].str.contains(region, case=False, na=False)]
                if filtered_data.empty:
                    return {"status": "error", "data": [f"No data found for the specified region: {region}."]}

            if start_year and end_year:
                year_columns = [str(year) for year in range(start_year, end_year + 1) if str(year) in filtered_data.columns]
                if year_columns:
                    filtered_data = filtered_data[['Region'] + year_columns].dropna()
                else:
                    return {"status": "error", "data": ["No data available for the specified year range."]}

            if analysis_type == "change":
                filtered_data['Change'] = filtered_data.iloc[:, 1:].diff(axis=1).sum(axis=1)
                filtered_data = filtered_data[['Region', 'Change']]
            elif analysis_type == "seasonality":
                if 'Seasonality' in filtered_data.columns:
                    filtered_data = filtered_data[['Region', 'Seasonality']].dropna()
                else:
                    return {"status": "error", "data": ["Seasonality data is not available for the selected region."]}
            else:
                filtered_data = filtered_data.dropna()

            if filtered_data.empty:
                return {"status": "error", "data": ["No data available for the specified region or criteria."]}

            return {"status": "success", "data": filtered_data.to_dict(orient='records')}
        
        except Exception as e:
            return {"status": "error", "data": [str(e)]}
