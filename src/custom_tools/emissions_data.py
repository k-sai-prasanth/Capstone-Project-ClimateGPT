from typing import Dict, Any, Optional, List, Union
import pandas as pd
from commons.custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam

# Load the emissions dataset globally or within the class as needed
emissions_data = pd.read_csv('../Datasets/GreenHouseEmissions.csv')

class EmissionDataTool(SingleMessageCustomTool):
    """
    Tool to get emission data for a given country, year, and emission type.
    Retrieve emission values for a specified country, year, and emission type. 
    The function will return a JSON object with the requested information. 
    If multiple countries, emission types, or specific years are queried, the function will provide the corresponding outputs for each, delivering only the specific data requested.
    If the requested emission type does not exist, it will return all emission types for the given country and year and suggest likely emission types.
    If 'all' is passed for country, year, or emission_type, the function will include all available information for that parameter.
    """

    def get_name(self) -> str:
        return "get_emission_data"

    def get_description(self) -> str:
        return "Get emission values for a given country, year, and emission type."

    def get_params_definition(self) -> Dict[str, ToolParamDefinitionParam]:
        return {
            "country": ToolParamDefinitionParam(
                param_type="union",
                description="The name of the country or a list of countries, or 'all' to include all countries.",
                required=True,
            ),
            "year": ToolParamDefinitionParam(
                param_type="union",
                description="The year or a list of years of the emission data, or 'all' to include all years.",
                required=False,
            ),
            "emission_type": ToolParamDefinitionParam(
                param_type="union",
                description="The emission type or a list of emission types, or 'all' to include all emission types. E.g., 'sfc_emissions', 'n2o_emissions', 'green_house_emissions', etc.",
                required=False,
            ),
        }

    async def run_impl(self, country: Union[str, List[str]], year: Optional[Union[int, List[int]]] = None, emission_type: Optional[Union[str, List[str]]] = None) -> Optional[Any]:
        # Handle 'all' for country
        if country == "all":
            all_countries = emissions_data['Country or Area'].unique().tolist()
            filtered_data = emissions_data
        else:
            # Ensure country is a list for consistent processing
            if isinstance(country, str):
                country = [country]
            # Filter the dataset for the specified countries
            filtered_data = emissions_data[emissions_data['Country or Area'].isin(country)]

        # Handle 'all' for year
        if year == "all":
            all_years = emissions_data['Year'].unique().tolist()
        else:
            if year:
                if isinstance(year, int):
                    year = [year]
                filtered_data = filtered_data[filtered_data['Year'].isin(year)]
            else:
                all_years = None

        # Handle 'all' for emission type
        if emission_type == "all":
            all_emission_types = emissions_data.columns[3:].tolist()  # Assuming emission columns start after the third column
        else:
            if emission_type:
                if isinstance(emission_type, str):
                    emission_type = [emission_type]

                missing_emission_types = [etype for etype in emission_type if etype not in emissions_data.columns]

                if missing_emission_types:
                    available_emission_types = ['sfc_emissions', 'n2o_emissions', 'methane_emissions', 'green_house_emissions']
                    return {
                        "message": f'Emission type(s) "{missing_emission_types}" do not exist. You might want to request any of the following: {", ".join(available_emission_types)}.',
                        "data": filtered_data[['Country or Area', 'Year'] + available_emission_types].to_json(orient="records", date_format="iso")
                    }

                # Filter only the requested emission types and return
                data = filtered_data[['Country or Area', 'Year'] + emission_type]
            else:
                all_emission_types = None

        # If 'all' is passed, include all data for that parameter
        if country == "all" or year == "all" or emission_type == "all":
            return {
                "countries": all_countries if country == "all" else country,
                "years": all_years if year == "all" else year,
                "emission_types": all_emission_types if emission_type == "all" else emission_type,
                "data": filtered_data.to_json(orient="records", date_format="iso")
            }
        else:
            # Return the result as a JSON object for the filtered data
            return data.to_json(orient="records", date_format="iso")