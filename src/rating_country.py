import os
import sys
from typing import Dict, Any, Optional, List, Union
import pandas as pd
from custom_tools import SingleMessageCustomTool
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam

# Load the emissions dataset globally or within the class as needed
emissions_data = pd.read_csv('../Datasets/country_rating_data.csv')

class RatingCountryTool(SingleMessageCustomTool):
    """
    Tool to get rating for a given country for different components.
    Retrieve rating for given country for a specified component. Components can be Policies and Action, Domestic or supported target, Fair share target, Climate finance or Net zero target.
    Retrieve overall rating for a given country
    The function will return a JSON object with the requested information. 
    If multiple countries, multiple components are requested, delivering only the specific data requested.
    If the requested country or sector type does not exist, it will return all sector types for the given country and year.
    If 'all' is passed for country and components. All requested information are returned in JSON format.
    """

    def get_name(self) -> str:
        return "get_country_rating"

    def get_description(self) -> str:
        return "Get sector country rating for a given country"

    def get_params_definition(self) -> Dict[str, ToolParamDefinitionParam]:
        return {
            "Country": ToolParamDefinitionParam(
                param_type="union",
                description="The name of the country or a list of countries, or 'all' to include all countries.",
                required=True,
            ),
            "Component": ToolParamDefinitionParam(
                param_type="union",
                description="It can be either Overall rating (Overall rating of the country depending on what they have done to control emissions), Policies and action (Explanation: Policies and action refer to what is actually happening in a country to reduce emissions - what emissions levels do we expect if all current policies are fully implemented. We rate a government's policies and action against the framework that is most favourable to it - fair share or modelled domestic pathways), Domestic or supported target(The CAT rates countries' Nationally Determined Contribution (NDC) targets against indicative national emissions from global least-cost emissions pathways (called modelled domestic pathways). For countries that should be supporting others, or can do it alone, we evaluate the domestic part - what it will do on their own territory - of its (NDC). For countries that could expect to receive support to fully decarbonise, we evaluate the conditional NDC; what a government plans to do if it receives international support. Countries that do not have a conditional NDC are rated using their unconditional NDC and are encouraged to develop a conditional NDC, outlining the support they need. If an NDC doesn't specify that part of the emissions reduction target is to be achieved outside the country's own borders, we assume that the NDC target is domestic only), Fair share target (The CAT rates NDC targets against each country's fair share contribution to global climate change mitigation, considering a range of equity principles including responsibility, capability, and equality. For assessing targets against the fair share, we consider both a country's domestic emission reductions and any emissions it supports abroad through the use of market mechanisms or other ways of support, as relevant. Here we evaluate what a government has promised to do with its own resources within its own territory or outside against our fair share pathways), Climate finance (A government whose fair share obligations are difficult, or even impossible, to meet with its target on its own territory is expected to meet its fair share internationally through funding and supporting emission reductions in other countries through direct financial resources. We include here only the direct financial transfers; the implied transfers accounted against the reduction target are already covered under the fair share target. For assessing targets against the fair share, we consider both a country's domestic emission reductions and its climate finance rating), Net zero target(We provide an assessment of the comprehensiveness and transparency of governments' net zero targets as part of their complete climate action efforts based on our ten-step good practice evaluation methodology.)",
                required=False,
            ),
        }

    async def run_impl(self, Country: Union[str, List[str]], Component: Optional[Union[str, List[str]]] = None) -> Optional[Any]:
        # Handle 'all' for Country
        if Country == "all":
            filtered_data = emissions_data
        else:
            # Ensure Country is a list for consistent processing
            if isinstance(Country, str):
                Country = [Country]
            # Filter the dataset for the specified countries
            filtered_data = emissions_data[emissions_data['Country'].isin(Country)]

        # Handle 'all' for Component
        if Component == "all" or Component is None:
            selected_columns = emissions_data.columns.tolist()
        else:
            # Split the component string by commas if it’s a single string with multiple components
            if isinstance(Component, str):
                Component = [comp.strip() for comp in Component.split(",")]

            # Filter for the specified components and include "Country" in the selected columns
            selected_columns = ["Country"] + [comp for comp in Component if comp in emissions_data.columns]

        # Select only the necessary columns based on the Component and remove duplicates
        result_data = filtered_data[selected_columns].drop_duplicates()

        # Return the result as a JSON object
        return result_data.to_json(orient="records", date_format="iso")