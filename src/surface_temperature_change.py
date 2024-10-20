import pandas as pd
from typing import Dict, Any, List
from custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam

# Load the temperature data globally, allowing access across all instances
data = pd.read_csv('../Datasets/Climate_Indicators.csv')

class SurfaceTemperatureChangeTool(SingleMessageCustomTool):
    """
    Tool to retrieve surface temperature changes based on specified criteria, including country, year, period, threshold, decade, and regional analysis.
    """

    def get_name(self) -> str:
        """Returns the name of the tool used for invocation."""
        return "get_surface_temperature_change"

    def get_description(self) -> str:
        """
        Tool Description
        
        This tool allows the user to query earth's surface temperature change data for one or more countries or regions, 
        either for a specific year or over a defined period. It supports multiple operations, such as comparing countries, 
        finding top 'n' countries with highest or lowest temperature changes, retrieving data that crosses a certain threshold, 
        analyzing data over decades, or aggregating data for regions.
        """
        return (
            "Retrieve surface temperature change data for one or more specified countries or regions, "
            "optionally filtered by a specific year, range of years, decade, or a threshold value. "
            "The tool provides information in degrees Celsius for the requested parameters, including "
            "decade-level and regional-level analysis."
        )

    def get_params_definition(self) -> Dict[str, ToolParamDefinitionParam]:
        return {
            "country": ToolParamDefinitionParam(
                param_type="str",
                description=(
                    "The country or list of countries for which to fetch temperature change data. "
                    "For example, 'India' or 'India, Brazil'. If not provided, data for all countries will be aggregated."
                ),
                required=False,
            ),
            "region": ToolParamDefinitionParam(
                param_type="str",
                description=(
                    "The region or list of regions for which to fetch temperature change data. "
                    "For example, 'Africa' or 'Asia, Europe'. If not provided, data for all regions will be aggregated."
                ),
                required=False,
            ),
            "start_year": ToolParamDefinitionParam(
                param_type="int",
                description="The starting year for the data. If not provided, it will include all available years.",
                required=False,
            ),
            "end_year": ToolParamDefinitionParam(
                param_type="int",
                description="The ending year for the data. If not provided, it will include all available years.",
                required=False,
            ),
            "threshold": ToolParamDefinitionParam(
                param_type="float",
                description="Temperature change threshold in degrees Celsius. Used to filter results.",
                required=False,
            ),
            "top_n": ToolParamDefinitionParam(
                param_type="int",
                description="The number of top countries or regions to retrieve based on temperature change. Default is 5.",
                required=False,
            ),
            "ascending": ToolParamDefinitionParam(
                param_type="bool",
                description="If True, returns countries or regions with the lowest anomalies, else returns the highest.",
                required=False,
            ),
            "decade_start": ToolParamDefinitionParam(
                param_type="int",
                description="The starting year of the decade for decade-level analysis. Should be a multiple of 10.",
                required=False,
            ),
        }

    async def run_impl(self, country: str = None, region: str = None, start_year: int = None, end_year: int = None,
                       threshold: float = None, top_n: int = 5, ascending: bool = False,
                       decade_start: int = None) -> Dict[str, Any]:
        """
        Execute the tool's main logic to retrieve surface temperature change data.
        
        The tool fetches data based on the specified criteria such as country, region, year range, 
        temperature threshold, or decade analysis, and returns aggregated information.
        """
        try:
            # Filter by country if specified
            filtered_data = data.copy()
            if country:
                country_list = [c.strip() for c in country.split(',')]
                filtered_data = filtered_data[filtered_data['Country'].isin(country_list)]
                if filtered_data.empty:
                    return {"status": "error", "data": [f"No data available for country/countries: {', '.join(country_list)}"]}

            # Filter by region if specified
            if region:
                region_list = [r.strip() for r in region.split(',')]
                filtered_data = filtered_data[filtered_data['Country'].isin(region_list)]
                if filtered_data.empty:
                    return {"status": "error", "data": [f"No data available for region/regions: {', '.join(region_list)}"]}

            # Filter by year range if specified
            if start_year and end_year:
                years = [str(year) for year in range(start_year, end_year + 1)]
                filtered_data = filtered_data[['Country'] + years].dropna(how='all', subset=years)
                if filtered_data.empty:
                    return {"status": "error", "data": ["No data available for the specified year range."]}

            # Apply threshold filtering if specified
            if threshold is not None:
                filtered_data = filtered_data.loc[filtered_data[years].max(axis=1) > threshold]
                if filtered_data.empty:
                    return {"status": "error", "data": [f"No countries or regions found with temperature change exceeding {threshold} degrees Celsius."]}

            # Handle top_n request for countries or regions with highest or lowest temperature changes
            if start_year and end_year:
                # Get the average temperature change over the period
                filtered_data['AverageChange'] = filtered_data[years].mean(axis=1)
                sorted_data = filtered_data.sort_values(by='AverageChange', ascending=ascending)
                result = list(zip(sorted_data['Country'], sorted_data['AverageChange']))[:top_n]
                return {"status": "success", "data": result}

            # Handle decade analysis if specified
            if decade_start is not None:
                decade_years = [str(year) for year in range(decade_start, decade_start + 10)]
                filtered_data = filtered_data[['Country'] + decade_years].dropna(how='all', subset=decade_years)
                if filtered_data.empty:
                    return {"status": "error", "data": [f"No data available for the decade starting in {decade_start}."]}
                filtered_data['DecadeAverage'] = filtered_data[decade_years].mean(axis=1)
                result = filtered_data[['Country', 'DecadeAverage']].to_dict(orient='records')
                return {"status": "success", "data": result}

            # Default return if no specific year range or decade analysis is requested
            return {"status": "success", "data": filtered_data.to_dict(orient='records')}

        except Exception as e:
            return {"status": "error", "data": [str(e)]}
