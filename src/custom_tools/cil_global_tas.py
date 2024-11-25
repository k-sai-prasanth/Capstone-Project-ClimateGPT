import pandas as pd
from typing import Dict, Any, List
from commons.custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam

class CIL_Global_TAS_Tool(SingleMessageCustomTool):
    def __init__(self, data: pd.DataFrame = None):
        self.data = data if data is not None else pd.read_csv("../Datasets/cil_global_tas_annual.csv")
        self.jja_data = data if data is not None else pd.read_csv("../Datasets/cil_global_tas_JJA.csv")
        self.djf_data = data if data is not None else pd.read_csv("../Datasets/cil_global_tas_DJF.csv")
        self.u32_data = data if data is not None else pd.read_csv("../Datasets/cil_global_tasmin_under32F.csv")
        self.o95_data = data if data is not None else pd.read_csv("../Datasets/cil_global_tasmax_over95F.csv")

    def get_name(self) -> str:
        """
        Returns the name of the tool used for invocation.
        """
        return "get_cil_gobal_tas"

    def get_description(self) -> str:
        """
        Tool Description

        This tool is designed to query and process data from Climate Impact Lab's (CIL) 
        global temperature datasets. It supports filtering based on multiple parameters 
        such as season, country, years, SSP ranges, and percentiles. Additionally, it allows 
        grouping, aggregation, and sorting of the data for advanced analysis.
        """
        return (
            "This tool queries global temperature datasets from Climate Impact Lab. "
            "Users can filter the data by season, country, year range, SSP range, and percentiles. "
            "It supports aggregations and grouping operations and outputs the results as a structured dataset."
        )

    def get_parameters(self) -> Dict[str, ToolParamDefinitionParam]:
        """Defines the parameters for the tool."""
        return {
            "flag": ToolParamDefinitionParam(
                type="str",
                description=(
                    "Dataset flag indicating the data subset to use. Options are:\n"
                    "'jja' - June, July, August (summer data)\n"
                    "'djf' - December, January, February (winter data)\n"
                    "'u32' - Days with temperature below 32°F\n"
                    "'o95' - Days with temperature above 95°F\n"
                    "None - Use annual global data"
                ),
                required=False
            ),
            "countries": ToolParamDefinitionParam(
                type="list[str]",
                description="List of countries to filter data.",
                required=False
            ),
            "years_range": ToolParamDefinitionParam(
                type="list[str]",
                description=(
                    "List of year ranges (e.g., '2040-2059') to filter the data."
                    "Avialable year ranges are: '1986-2005', '2020-2039', '2040-2059', '2080-2099'"
                ),
                required=False
            ),
            "ssp_range": ToolParamDefinitionParam(
                type="list[str]",
                description=(
                    "List of SSP (Shared Socioeconomic Pathways) ranges (e.g., 'SSP2-4.5'). "
                    "Used to filter data by SSP scenarios."
                    "Available SSP ranges are: 'SSP2-4.5', 'SSP3-7.0', 'SSP5-8.5'"
                ),
                required=False
            ),
            "percentiles": ToolParamDefinitionParam(
                type="list[int]",
                description=(
                    "List of percentiles to include in the output. Options are:\n"
                    "'0.05' - 5th percentile\n"
                    "'0.50' - Median (50th percentile)\n"
                    "'0.95' - 95th percentile"
                    "Avialble percentiles are: 0.05, 0.50, 0.95"
                ),
                required=False
            ),
            "group_by": ToolParamDefinitionParam(
                type="List[str]",
                description=(
                    "Column name(s) to group the data by (e.g., ['country', 'ssp_range'])."
                    "Avialable group_by options are: 'country', 'ssp_range', 'year_range'."
                ),
                required=False
            ),
            "aggregate": ToolParamDefinitionParam(
                type="str",
                description=(
                    "Aggregation method to apply when grouping data. Options include 'mean', 'median', 'var', 'sem', 'min' and 'max'."
                ),
                required=False
            ),
            "sort": ToolParamDefinitionParam(
                type="str",
                description=(
                    "Sorting order for the output data. Options are:\n"
                    "'ascending' - Sort in ascending order\n"
                    "'descending' - Sort in descending order"
                ),
                default="descending",
                required=False
            ),
            "n": ToolParamDefinitionParam(
                type="int",
                description="Number of top records to return after filtering and sorting. 0 indicates return all.",
                required=True
            ),
        }

    async def run_impl(
        self,
        flag: str = None,
        countries: List[str] = None,
        years_range: List[str] = None,
        ssp_range: List[str] = None,
        percentiles: List[str] = None,
        group_by: List[str] = None,
        aggregate: str = None,
        sort: str = 'descending',
        n: int = 0,
    ) -> Dict[str, Any]:

        if flag == 'jja':
            filtered_data = self.jja_data.copy()
        elif flag == 'djf':
            filtered_data = self.djf_data.copy()
        elif flag == 'u32':
            filtered_data = self.u32_data.copy()
        elif flag == 'o95':
            filtered_data = self.o95_data.copy()
        else:
            filtered_data = self.data.copy()
        
        messages = []
        print("Start: \n", filtered_data)

        # Step 1: Filter by countries
        if countries:
            available_countries = filtered_data['country'].unique()
            missing_countries = [c for c in countries if c not in available_countries]
            if missing_countries:
                messages.append(f"Data for the following countries isn't available: {', '.join(missing_countries)}")
            filtered_data = filtered_data[filtered_data['country'].isin(countries)]
        
        print("Step 1 Filtered Data by countries: \n", filtered_data.head(5))

        # Step 2: Filter by years range
        if years_range:
            available_years = filtered_data['year_range'].unique()
            missing_years = [y for y in years_range if y not in available_years]
            if missing_years:
                messages.append(f"Data for the following year ranges isn't available: {', '.join(missing_years)}")
            filtered_data = filtered_data[filtered_data['year_range'].isin(years_range)]
        
        print("Step 2 Filtered Data by years range: \n", filtered_data.head(5))

        # Step 3: Filter by SSP ranges
        if ssp_range:
            available_ssp = filtered_data['ssp_range'].unique()
            missing_ssp = [s for s in ssp_range if s not in available_ssp]
            if missing_ssp:
                messages.append(f"Data for the following SSP ranges isn't available: {', '.join(missing_ssp)}")
            filtered_data = filtered_data[filtered_data['ssp_range'].isin(ssp_range)]

        print("Step 3 Filtered Data by SSP range: \n", filtered_data.head(5))

        # Step 4: Filter by percentiles
        selected_columns = ['country', 'ssp_range', 'year_range']

        if percentiles is None:
            # Default: Include all percentiles
            selected_columns += ['p0.05', 'p0.50', 'p0.95']
            filtered_data = filtered_data[selected_columns]
        else:
            # Validate provided percentiles
            invalid_percentiles = [p for p in percentiles if p not in ['0.05', '0.50', '0.95']]
            percentiles = [p for p in percentiles if p in ['0.05', '0.50', '0.95']]
            print(invalid_percentiles)
            if invalid_percentiles:
                # Append error message for invalid percentiles
                messages.append(f"Invalid percentile value(s): {', '.join(invalid_percentiles)}")
            if percentiles:
                # Include only valid percentiles
                selected_percentiles = [f"p{percentile}" for percentile in percentiles]
                selected_columns += selected_percentiles
                filtered_data = filtered_data[selected_columns]
            else:
                selected_columns = []
                filtered_data = filtered_data[selected_columns]

        print("Step 4 Filtered Data by SSP range: \n", filtered_data.head(5))

        # Step 4: Perform aggregations
        if group_by:
            # Apply aggregation based on grouping
            filtered_data = await self.aggregate_data(filtered_data, aggregate, group_by)
            print("Step 5 Filtered Data with group_by and aggregation: \n", filtered_data.head(5))   
        else:
            if aggregate:
                filtered_data = await self.aggregate_data(filtered_data, aggregate)
                print("Step 5 Filtered Data with only aggregation: \n", filtered_data.head(5))  
            else:
                print("Step 5 No aggregation.") 
        
        # Step 5: sorting
        filtered_data = await self.sort_data(filtered_data, sort)
        print("Step 6 Filtered Data after sorting: \n", filtered_data.head(5))   
        
        if n!=0:
            filtered_data = filtered_data.head(n)

        result = filtered_data.to_dict(orient='records')
        # Format output
        output = {
            'data': result,
            'messages': messages
        }
        return output
    
    ### Helper Methods ###
    async def aggregate_data(self, data: pd.DataFrame, aggregate: str, group_by: List[str] = None) -> pd.DataFrame:
        
        agg_dict = {col: aggregate for col in data.columns if col.startswith('p')}
        if group_by:
            selected_columns = ['p0.05', 'p0.50', 'p0.95']
            selected_columns += group_by
            print(selected_columns)
            data = data[selected_columns] 
            result = data.groupby(group_by).agg(agg_dict).reset_index()
        else:
            result = data.agg(agg_dict).reset_index()
        
        print(result)
        if isinstance(result.columns, pd.MultiIndex):
            result.columns = result.columns.get_level_values(0)

        return result   
    
    async def sort_data(self, data: pd.DataFrame, sort: str) -> pd.DataFrame:
        sort_columns = [col for col in data.columns if col.startswith('p')]
        ascending = sort != 'descending'
        sorted_data = data.sort_values(by=sort_columns, ascending=ascending)
        
        return sorted_data