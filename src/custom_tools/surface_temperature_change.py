import pandas as pd
from typing import Dict, Any, List
from commons.custom_tools import SingleMessageCustomTool
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
            "command": ToolParamDefinitionParam(
                param_type="str",
                description=(
                    "It has list of commands to chose based on the question type."
                    "The available commands are temperature_change_for_country, temperature_change_between_years, compare_temperature_change, top_n_temperature_change, threshold_exceeded"
                    "For example if question is regarding the surface temperature change for a particular country, then command = temperature_change_for_country."
                ),
                required=False,
            ),
            "country": ToolParamDefinitionParam(
                param_type="str",
                description=(
                    "The country or list of countries for which to fetch temperature change data. "
                    "For example, 'India' or 'India, Brazil'. If not provided, data for all countries will be aggregated."
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
            "interval": ToolParamDefinitionParam(
                param_type="int",
                description= "The interval or shift in years. Used to get data at every 'interval' years starting from 'start_year'.",
                required=False,
            ),
        }

    async def run_impl(self, command: str, country: str = None, start_year: int = None, end_year: int = None,
                       threshold: float = None, top_n: int = 5, ascending: bool = False,
                       decade_start: int = None, interval:int = None) -> Dict[str, Any]:
        try:
            filtered_data = data.copy()
                
            if command == "temperature_change_for_country":
                if country:
                    country_list = [c.strip() for c in country.split(',')]
                    filtered_data = filtered_data[filtered_data['Country'].isin(country_list)]
                    if start_year:
                        if interval:
                            # Generate years at the specified interval starting from start_year
                            years = [str(year) for year in range(start_year, int(filtered_data.columns[-1]) + 1, interval) if str(year) in filtered_data.columns]
                            if not years:
                                return {"status": "error", "data": ["No data available for the specified years."]}
                            filtered_data = filtered_data[['Country'] + years].dropna()
                        else:
                            if str(start_year) in filtered_data.columns:
                                filtered_data = filtered_data[['Country', str(start_year)]].dropna()
                            else:
                                return {"status": "error", "data": ["No data available for the specified year."]}
                    else:
                        filtered_data['TotalChange'] = filtered_data.iloc[:, 1:].sum(axis=1)
                        filtered_data = filtered_data[['Country', 'TotalChange']]
                    if filtered_data.empty:
                        return {"status": "error", "data": ["No data available for the specified country."]}
                    return {"status": "success", "data": filtered_data.to_dict(orient='records')}

            elif command == "temperature_change_between_years":
                if country and start_year and end_year:
                    country_list = [c.strip() for c in country.split(',')]
                    filtered_data = filtered_data[filtered_data['Country'].isin(country_list)]
                    years = [str(year) for year in range(start_year, end_year + 1)]
                    filtered_data['SumChange'] = filtered_data[years].sum(axis=1)
                    filtered_data = filtered_data[['Country', 'SumChange']]
                    if filtered_data.empty:
                        return {"status": "error", "data": ["No data available for the specified country or year range."]}
                    return {"status": "success", "data": filtered_data.to_dict(orient='records')}

            elif command == "compare_temperature_change":
                if country:
                    country_list = [c.strip() for c in country.split(',')]
                    filtered_data = filtered_data[filtered_data['Country'].isin(country_list)]
                    if start_year and end_year:
                        years = [str(year) for year in range(start_year, end_year + 1)]
                        filtered_data['AverageChange'] = filtered_data[years].mean(axis=1)
                        filtered_data = filtered_data[['Country', 'AverageChange']]
                    elif decade_start is not None:
                        decade_years = [str(year) for year in range(decade_start, decade_start + 10)]
                        filtered_data['DecadeAverage'] = filtered_data[decade_years].mean(axis=1)
                        filtered_data = filtered_data[['Country', 'DecadeAverage']]
                    elif start_year:
                        if str(start_year) in filtered_data.columns:
                            filtered_data = filtered_data[['Country', str(start_year)]]
                        else:
                            return {"status": "error", "data": ["No data available for the specified year."]}
                    if filtered_data.empty:
                        return {"status": "error", "data": ["No data available for the specified countries or time range."]}
                    return {"status": "success", "data": filtered_data.to_dict(orient='records')}

            elif command == "top_n_temperature_change":
                if start_year and end_year:
                    years = [str(year) for year in range(start_year, end_year + 1)]
                    filtered_data['SumChange'] = filtered_data[years].sum(axis=1)
                    sorted_data = filtered_data.sort_values(by='SumChange', ascending=ascending)
                    result = list(zip(sorted_data['Country'], sorted_data['SumChange']))[:top_n]
                    return {"status": "success", "data": result}
                elif start_year:
                    if str(start_year) in filtered_data.columns:
                        sorted_data = filtered_data.sort_values(by=str(start_year), ascending=ascending)
                        result = list(zip(sorted_data['Country'], sorted_data[str(start_year)]))[:top_n]
                        return {"status": "success", "data": result}
                elif decade_start is not None:
                    decade_years = [str(year) for year in range(decade_start, decade_start + 10)]
                    filtered_data['SumChange'] = filtered_data[decade_years].sum(axis=1)
                    sorted_data = filtered_data.sort_values(by='SumChange', ascending=ascending)
                    result = list(zip(sorted_data['Country'], sorted_data['SumChange']))[:top_n]
                    return {"status": "success", "data": result}
                else:
                    return {"status": "error", "data": ["Please specify a valid year, range, or decade."]}

            elif command == "threshold_exceeded":
                if threshold is not None:
                    if start_year and end_year:
                        years = [str(year) for year in range(start_year, end_year + 1)]
                        filtered_data['AverageChange'] = filtered_data[years].mean(axis=1)
                        filtered_data = filtered_data[filtered_data['AverageChange'] >= threshold]
                    elif start_year:
                        if str(start_year) in filtered_data.columns:
                            filtered_data = filtered_data[filtered_data[str(start_year)] >= threshold]
                    elif decade_start is not None:
                        decade_years = [str(year) for year in range(decade_start, decade_start + 10)]
                        filtered_data['AverageChange'] = filtered_data[decade_years].mean(axis=1)
                        filtered_data = filtered_data[filtered_data['AverageChange'] >= threshold]
                    if filtered_data.empty:
                        return {"status": "error", "data": ["No countries found exceeding the specified threshold."]}
                    return {"status": "success", "data": filtered_data.to_dict(orient='records')}
                else:
                    return {"status": "error", "data": ["Please specify a valid threshold value."]}

            else:
                return {"status": "error", "data": ["Invalid command specified."]}

        except Exception as e:
            return {"status": "error", "data": [str(e)]}
