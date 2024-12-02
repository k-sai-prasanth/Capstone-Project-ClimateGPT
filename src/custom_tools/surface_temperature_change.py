import pandas as pd
from typing import Dict, Any, List
from commons.custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam

class SurfaceTemperatureChangeTool(SingleMessageCustomTool):
    """
    Tool to retrieve surface temperature changes based on specified criteria, including country, year, period, threshold, decade, and regional analysis.
    """

    def __init__(self, data: pd.DataFrame = None):
        """Initialize the tool with an optional data parameter."""
        self.data = data if data is not None else pd.read_csv('../Datasets/Climate_Indicators.csv')
    
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
            "compare_years_difference": ToolParamDefinitionParam(
                param_type="bool",
                description=(
                    "Set to True to compare temperature changes between two years specified by 'start_year' and 'end_year', "
                    "filtering results based on 'difference_threshold' and 'increase_only'."
                ),
                required=False,
            ),
            "difference_threshold": ToolParamDefinitionParam(
                param_type="float",
                description=(
                    "The threshold for the difference in temperature change between 'start_year' and 'end_year'. "
                    "Used when 'compare_years_difference' is True."
                ),
                required=False,
            ),
            "increase_only": ToolParamDefinitionParam(
                param_type="bool",
                description=(
                    "When comparing years, if True, considers only positive increases exceeding the 'difference_threshold'. "
                    "If False, considers any absolute change exceeding the threshold."
                ),
                required=False,
            ),
        }

    async def run_impl( self, country: str = None, start_year: int = None, end_year: int = None, threshold: float = None, top_n: int = None, ascending: bool = False,
                       decade_start: int = None, interval: int = None, compare_years_difference: bool = False, 
                       difference_threshold: float = None, increase_only: bool = True ) -> Dict[str, Any]:
        try:
            # Create a copy of the dataset
            filtered_data = self.data.copy()
            if self.data.empty:
                return {"status": "error", "data": ["No data available for the specified parameters."]}

            # Filter by country if provided
            if country:
                country_list = [c.strip() for c in country.split(',')]
                filtered_data = filtered_data[filtered_data['Country'].isin(country_list)]
                if filtered_data.empty:
                    return {"status": "error", "data": ["No data available for the specified country(s)."]}
            
            # Determine the years to include
            years = self.determine_years(filtered_data, start_year, end_year, decade_start, interval)
            if not years:
                return {"status": "error", "data": ["No valid years specified or years not found in the dataset."]}
            
            # Handle comparison between two years with a difference threshold
            if compare_years_difference and start_year and end_year and difference_threshold is not None:
                return self.compare_years_with_difference(filtered_data, start_year, end_year, difference_threshold, increase_only)
            
            # Handle threshold queries
            elif threshold is not None:
                data_with_metric = self.calculate_metric(filtered_data, years, metric='average')
                data_with_metric = data_with_metric[data_with_metric['Metric'] >= threshold]
                if data_with_metric.empty:
                    return {"status": "error", "data": ["No countries found exceeding the specified threshold."]}
                return {"status": "success", "data": data_with_metric.to_dict(orient='records')}
            
            # Handle top_n queries
            elif top_n is not None:
                data_with_metric = self.calculate_metric(filtered_data, years, metric='sum')
                data_with_metric = data_with_metric.sort_values(by='Metric', ascending=ascending)
                data_with_metric = data_with_metric.head(top_n)
                if data_with_metric.empty:
                    return {"status": "error", "data": ["No data available after sorting."]}
                return {"status": "success", "data": data_with_metric.to_dict(orient='records')}
            
            # Default action: return the data for the specified years
            else:
                data_with_metric = filtered_data[['Country'] + years].dropna()
                if data_with_metric.empty:
                    return {"status": "error", "data": ["No data available for the specified parameters."]}
                return {"status": "success", "data": data_with_metric.to_dict(orient='records')}
        
        except Exception as e:
            return {"status": "error", "data": [str(e)]}

    def determine_years(self, data, start_year=None, end_year=None, decade_start=None, interval=None):
    # Extract available years from the data columns, excluding 'Country'
        available_years = []
        for col in data.columns:
            if col != 'Country' and col.isdigit():
                available_years.append(int(col))
        years_set = set(available_years)

        # If no years are available, return an empty list
        if not available_years:
            return []

        # Determine the minimum and maximum years in the dataset
        data_min_year = min(available_years)
        data_max_year = max(available_years)

        # Determine years based on the specified parameters
        if interval:
            # If only 'interval' is specified
            if not start_year and not end_year:
                start = data_min_year
                end = data_max_year
            # If 'interval' and 'start_year' are specified
            elif start_year and not end_year:
                start = max(start_year, data_min_year)
                end = data_max_year
            # If 'interval', 'start_year', and 'end_year' are specified
            elif start_year and end_year:
                start = max(start_year, data_min_year)
                end = min(end_year, data_max_year)
            else:
                # Handle other cases if necessary
                start = data_min_year
                end = data_max_year

            # Generate the list of years with the specified interval
            years = [
                str(year)
                for year in range(start, end + 1, interval)
                if year in years_set
            ]
        else:
            # Original logic when 'interval' is not specified
            if start_year and end_year:
                years = [
                    str(year)
                    for year in range(start_year, end_year + 1)
                    if year in years_set
                ]
            elif start_year:
                years = [str(start_year)] if start_year in years_set else []
            elif decade_start:
                years = [
                    str(year)
                    for year in range(decade_start, decade_start + 10)
                    if year in years_set
                ]
            else:
                years = [str(year) for year in sorted(years_set)]
        return years

    def calculate_metric(self, data, years, metric='average'):
        if metric == 'average':
            data['Metric'] = data[years].mean(axis=1)
        elif metric == 'sum':
            data['Metric'] = data[years].sum(axis=1)
        elif metric == 'difference':
            data['Metric'] = data[years[-1]] - data[years[0]]
        else:
            data['Metric'] = data[years[0]]
        return data[['Country', 'Metric']].dropna()

    def compare_years_with_difference(self, data, year1, year2, threshold, increase_only=True):
        # Ensure the years are in string format
        year1_str = str(year1)
        year2_str = str(year2)
        
        # Check if the years exist in the dataset
        if year1_str not in data.columns or year2_str not in data.columns:
            return {"status": "error", "data": ["Specified years are not in the dataset."]}
        
        # Calculate the difference between the two years
        data['Difference'] = data[year2_str] - data[year1_str]
        
        # Apply threshold and direction
        if increase_only:
            data_filtered = data[data['Difference'] > threshold]
        else:
            data_filtered = data[abs(data['Difference']) > threshold]
        
        # Check if any data remains after filtering
        if data_filtered.empty:
            return {"status": "error", "data": ["No countries found with temperature change difference exceeding the specified threshold."]}
        
        # Return the results
        result = data_filtered[['Country', 'Difference']].to_dict(orient='records')
        return {"status": "success", "data": result}