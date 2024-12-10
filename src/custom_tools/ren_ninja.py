import pandas as pd
from typing import Dict, Any, List
from commons.custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam
import pycountry

class RenNinjaTool(SingleMessageCustomTool):
    def __init__(self, data: pd.DataFrame = None):
        self.pv_data = data if data is not None else pd.read_csv("../Datasets/ninja_pv_europe_v1.1_merra2.csv")
        self.wind_data = data if data is not None else pd.read_csv("../Datasets/ninja_wind_europe_v1.1_current_national.csv")
        self.wind_data_longterm = data if data is not None else pd.read_csv("../Datasets/ninja_wind_europe_v1.1_future_longterm_national.csv")
        self.wind_data_nearterm = data if data is not None else pd.read_csv("../Datasets/ninja_wind_europe_v1.1_future_nearterm_national.csv")

    def get_name(self) -> str:
        """
        Returns the name of the tool used for invocation.
        """
        return "get_ren_ninja"
        
    def get_description(self) -> str:
        """
        Returns a description of the get_ren_ninja tool.
        """
        return (
            "The `get_ren_ninja` tool filters and processes renewable energy data, including PV and wind datasets. "
            "It allows querying by energy type (PV or wind), filtering by country, date range, months, years, and hours, "
            "and supports data aggregation, grouping, merging, and sorting. The tool is versatile and designed for "
            "analyzing renewable energy trends at various temporal and geographical scales."
        )

    def get_parameters(self) -> Dict[str, ToolParamDefinitionParam]:
        """
        Returns the parameters for the get_ren_ninja tool.
        """
        return {
            "level": {
                "type": "str",
                "description": "Specifies the energy type: 'pv' for photovoltaic data or 'wind' for wind data.",
                "required": True,
            },
            "flag": {
                "type": "str",
                "description": (
                    "Applicable for wind data only. Specifies the wind farm stage: "
                    "'longterm' for long-term planning, 'nearterm' for projects under construction or planning approval, "
                    "or None for current wind farms."
                ),
                "required": False,
            },
            "countries": {
                "type": "List[str]",
                "description": "List of country codes to filter the data.",
                "required": False,
            },
            "dates": {
                "type": "List[str]",
                "description": "List of specific dates to filter the data (format: YYYY-MM-DD).",
                "required": False,
            },
            "months": {
                "type": "List[int]",
                "description": "List of months (1-12) to filter the data.",
                "required": False,
            },
            "years": {
                "type": "List[int]",
                "description": "List of years to filter the data.",
                "required": False,
            },
            "hours": {
                "type": "List[int]",
                "description": "List of hours (0-23) to filter the data.",
                "required": False,
            },
            "data_merge": {
                "type": "str",
                "description": (
                    "Specifies the level of data merging: 'daily', 'monthly', or 'yearly'. "
                    "Aggregates the capacity factors over the specified time scale."
                ),
                "required": False,
            },
            "group_by": {
                "type": "str",
                "description": (
                    "Groups data by a specific column, e.g., 'country' or 'time'. "
                    "Supports aggregations within the group."
                ),
                "required": False,
            },
            "aggregate": {
                "type": "str",
                "description": "Specifies the aggregation method (eg: 'mean', 'sum', 'max', 'min').",
                "required": False,
            },
            "sort": {
                "type": "str",
                "description": "Sort order for capacity factor data: 'ascending' or 'descending' (default: 'descending').",
                "default": "descending",
                "required": False,
            },
            "n": {
                "type": "int",
                "description": "Limits the result to the top N rows based on sorting.",
                "required": False,
            },
        }

    async def run_impl(
        self,
        level = str,
        flag: str = None,
        countries: List[str] = None,
        dates: List[str] = None,
        months: List[int] = None,
        years: List[int] = None,
        hours: List[int] = None,
        data_merge: str = None,
        group_by: str = None,
        aggregate: str = None,
        sort: str = 'descending',
        n: int = None
    ) -> Dict[str, Any]:
        
        messages = []
        try:
            if level == 'pv':
                filtered_data = self.pv_data.copy()
            elif level == 'wind':
                if flag == 'longterm':
                    filtered_data = self.wind_data_longterm.copy()
                elif flag == 'nearterm':
                    filtered_data = self.wind_data_nearterm.copy()
                else:
                   filtered_data = self.wind_data.copy() 
            else:
                raise ValueError("Invalid level specified.")
            
        except ValueError as e:
            messages.append(f"Value error: {str(e)}")
            
        if(level in ['pv', 'wind']):
        
            filtered_data['time'] = pd.to_datetime(filtered_data['time'])
            messages = []
            print("Start: \n", filtered_data.head())

            filtered_data = await self.struct_data(filtered_data)

            print("Structured data:\n", filtered_data.head(15))

            # Step 1: Filter by countries
            if countries:
                available_countries = filtered_data['country'].unique()
                missing_countries = [c for c in countries if c not in available_countries]
                if missing_countries:
                    messages.append(f"Data for the following countries isn't available: {', '.join(missing_countries)}")
                filtered_data = filtered_data[filtered_data['country'].isin(countries)]
            
            print("Step 1 Filtered Data by countries: \n", filtered_data.head(5))

            # Step 2: Filter by dates, months, years, hours
            if dates:
                missing_dates = [d for d in dates if d not in filtered_data['time'].unique()]
                filtered_data = filtered_data[filtered_data['time'].dt.date.isin(pd.to_datetime(dates).date)]
                if missing_dates:
                    messages.append(f"Data for the following dates isn't available: {', '.join(missing_dates)}")
            
            if months:
                incorrect_months = [str(m) for m in months if m not in range(1,13)]
                filtered_data = filtered_data[filtered_data['time'].dt.month.isin(months)]
                if incorrect_months:
                    messages.append(f"Incorrect months: {', '.join(incorrect_months)}")
            
            if years:
                missing_years =  [str(y) for y in years if y not in filtered_data['time'].dt.year.unique()]
                filtered_data = filtered_data[filtered_data['time'].dt.year.isin(years)]
                if missing_years:
                    messages.append(f"Data for the following years isn't available: {', '.join(missing_years)}")

            if hours:
                incorrect_hours = [str(h) for h in hours if h not in range(0,24)]
                print(incorrect_hours)
                filtered_data = filtered_data[filtered_data['time'].dt.hour.isin(hours)]
                if incorrect_hours:
                    messages.append(f"Incorrect hours: {', '.join(incorrect_hours)}")

            print("Step 2 Filtered Data by time: \n", filtered_data.head(5))

            # Step 3: Perform data merging
            if data_merge:
                filtered_data = await self.merge_data(filtered_data, data_merge)
            
            print("Step 3 Filtered Data  after merging operation: \n", filtered_data.head(5))
            
            # Step 4: Perform aggregations
            if group_by:
                # Apply aggregation based on grouping
                filtered_data = await self.aggregate_data(filtered_data, aggregate, group_by)
                print("Step 4 Filtered Data with group_by and aggregation: \n", filtered_data.head(5))   
            else:
                if aggregate:
                    filtered_data = await self.aggregate_data(filtered_data, aggregate)
                    print("Step 4 Filtered Data with only aggregation: \n", filtered_data.head(5))  
                else:
                    print("Step 4 No aggregation.") 
            
            # Step 5: sorting
            filtered_data = await self.sort_data(filtered_data, sort)
            print("Step 5 Filtered Data after sorting: \n", filtered_data.head(5))   

            # Step 6: N rows
            if n:
                filtered_data = filtered_data.head(n)
            
            result = filtered_data.to_dict(orient='records')
            # Format output
            output = {
                'data': result,
                'messages': messages
            }
            return output
        
        output = {
                'data': None,
                'messages': messages
            }
        return output
    
    ### Helper Methods ###
    
    async def struct_data(self, data: pd.DataFrame) -> pd.DataFrame:
        
        data = pd.melt(data, id_vars=['time'], var_name='country', value_name='capacity_factor')
        
        # Create a dictionary mapping ISO alpha-2 codes to country names using pycountry
        # Create the mapping
        iso_to_country = {
            'time': 'time',  # Adding 'time' as a special key
            **{country.alpha_2: country.name for country in pycountry.countries}
        }
        data['country'] = data['country'].map(iso_to_country)
        
        return data


    async def merge_data(self, data: pd.DataFrame, data_merge: str) -> pd.DataFrame:
        
        # Perform resampling and aggregation based on `data_merge` level
        if data_merge == 'daily':
            result = data.groupby([data['time'].dt.date, 'country']).agg({'capacity_factor': 'mean'}).reset_index()
        elif data_merge == 'monthly':
            result = data.groupby([data['time'].dt.strftime('%Y-%m'), 'country']).agg({'capacity_factor': 'mean'}).reset_index()
        elif data_merge == 'yearly':
            result = data.groupby([data['time'].dt.year, 'country']).agg({'capacity_factor': 'mean'}).reset_index()
        
        return result

    async def aggregate_data(self, data: pd.DataFrame, aggregate: str, group_by: str = None) -> pd.DataFrame:
        
        if group_by == 'country':
            result = data.groupby('country').agg({'capacity_factor' : aggregate}).reset_index()
        elif group_by == 'time':
            result = data.groupby('time').agg({'capacity_factor' : aggregate}).reset_index()
        elif group_by == None:
            result = data.agg({'capacity_factor' : aggregate}).reset_index()
        
        
        if isinstance(result.columns, pd.MultiIndex):
            result.columns = result.columns.get_level_values(0)

        return result   
    
    async def sort_data(self, data: pd.DataFrame, sort: str) -> pd.DataFrame:
        sorted_data = data.sort_values(by='capacity_factor', ascending=(sort == 'ascending'))
        return sorted_data
