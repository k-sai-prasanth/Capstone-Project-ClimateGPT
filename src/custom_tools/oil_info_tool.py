import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Dict, Any, List, Union
from llama_stack_client.types.tool_param_definition_param import ToolParamDefinitionParam


class OilInfoTool:
    """
    Class to process climate-related queries using structured parameters.
    Handles operations such as filtering, aggregation, trend analysis, and prediction.
    """

    def __init__(self, data: pd.DataFrame = None):
        """Initialize the tool with an optional data parameter."""
        self.data = data if data is not None else pd.read_csv('../Datasets/oil_info.csv')

    def get_name(self) -> str:
        """Returns the name of the tool used for invocation."""
        return "get_oil_info"

    def get_description(self) -> str:
        """
        Tool Description

        This tool provides information about the different locations of oil and gas fields
        around the country, including the emissions produced by the gas.
        """
        return (
            "Retrieve information about different oil fields, "
            "with all details about each oil field."
        )

    def get_params_definition(self) -> Dict[str, ToolParamDefinitionParam]:
        """
        Defines the parameters required for tool invocation.
        """
        return {
            "filters": ToolParamDefinitionParam(
                param_type="dict",
                description="A dictionary of filters to apply to the dataset (e.g., {'Location': 'Onshore'})."
            ),
            "group_by": ToolParamDefinitionParam(
                param_type="str",
                description="The column to group data by (e.g., 'Country')."
            ),
            "agg_col": ToolParamDefinitionParam(
                param_type="str",
                description="The column to aggregate (e.g., 'Methane Intensity')."
            ),
            "agg_func": ToolParamDefinitionParam(
                param_type="str",
                description="The aggregation function to use (e.g., 'mean', 'sum')."
            ),
            "trend": ToolParamDefinitionParam(
                param_type="dict",
                description=(
                    "A dictionary specifying the columns for trend analysis "
                    "(e.g., {'x_col': 'Year', 'y_col': 'Emissions'})."
                )
            ),
            "predict": ToolParamDefinitionParam(
                param_type="dict",
                description=(
                    "A dictionary specifying prediction parameters "
                    "(e.g., {'x_col': 'Year', 'y_col': 'Emissions', 'future_x': 2030})."
                )
            ),
        }

    async def run_impl( self, filters: Dict[str, Union[str, List[str]]] = None, group_by: str = None, agg_col: str = None,
                        agg_func: str = None, trend: Dict[str, str] = None, predict: Dict[str, Union[str, float]] = None, ) -> Dict[str, Any]:
        """
        Execute the tool based on provided parameters.
        :return: The result of the operation (data or analysis).
        """
        try:
            # Step 1: Apply filters
            filtered_data = self.filter_data(filters) if filters else self.data

            # Step 2: Perform group and aggregate
            if group_by and agg_col and agg_func:
                result = self.group_and_aggregate(filtered_data, group_by, agg_col, agg_func)

            # Step 3: Perform trend analysis
            elif trend:
                x_col = trend["x_col"]
                y_col = trend["y_col"]
                slope = self.calculate_trend(filtered_data, x_col, y_col)
                result = {"trend_slope": f"The trend slope between {x_col} and {y_col} is {slope:.2f}"}

            # Step 4: Perform prediction
            elif predict:
                x_col = predict["x_col"]
                y_col = predict["y_col"]
                future_x = predict["future_x"]
                prediction = self.predict_future(filtered_data, x_col, y_col, future_x)
                result = {
                    "prediction": f"The predicted value of {y_col} for {x_col} = {future_x} is {prediction:.2f}"
                }

            # Step 5: Return filtered data if no other operation specified
            else:
                result = filtered_data.to_dict(orient="records")

            return {"status": "success", "result": result}

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def filter_data(self, filters: Dict[str, Union[str, List[str]]]) -> pd.DataFrame:
        """
        Filter the dataset based on given conditions.
        :param filters: Dictionary of column-value pairs to filter on.
        :return: Filtered DataFrame.
        """
        filtered_data = self.data
        for col, val in filters.items():
            if val == "all":
                continue
            if isinstance(val, list):
                filtered_data = filtered_data[filtered_data[col].isin(val)]
            else:
                filtered_data = filtered_data[filtered_data[col] == val]
        return filtered_data

    def group_and_aggregate(self, data: pd.DataFrame, group_col: str, agg_col: str, agg_func: str) -> pd.DataFrame:
        """
        Group and aggregate the dataset.
        :param data: The DataFrame to operate on.
        :param group_col: Column to group by.
        :param agg_col: Column to aggregate.
        :param agg_func: Aggregation function (e.g., 'sum', 'mean').
        :return: Aggregated DataFrame.
        """
        return data.groupby(group_col, as_index=False)[agg_col].agg(agg_func)


    def calculate_trend(self, data: pd.DataFrame, x_col: str, y_col: str) -> float:
        """
        Calculate trend analysis (linear regression slope) between two columns.
        :param data: The DataFrame to operate on.
        :param x_col: Independent variable.
        :param y_col: Dependent variable.
        :return: Slope of the trend line.
        """
        x = data[x_col]
        y = data[y_col]
        coefficients = np.polyfit(x, y, 1)
        return coefficients[0]

    def predict_future(self, data: pd.DataFrame, x_col: str, y_col: str, future_x: float) -> float:
        """
        Predict future values using linear regression.
        :param data: The DataFrame to operate on.
        :param x_col: Independent variable.
        :param y_col: Dependent variable.
        :param future_x: Value of the independent variable to predict for.
        :return: Predicted value of the dependent variable.
        """
        x = data[x_col]
        y = data[y_col]
        coefficients = np.polyfit(x, y, 1)
        return coefficients[0] * future_x + coefficients[1]


    def visualize_results(self, x_col: str, y_col: str, title: str = "Result Visualization"):
        """
        Create a scatter plot for visualization.
        :param x_col: X-axis column.
        :param y_col: Y-axis column.
        :param title: Title of the plot.
        """
        plt.figure(figsize=(10, 6))
        plt.scatter(self.data[x_col], self.data[y_col], color="blue", alpha=0.6)
        plt.title(title)
        plt.xlabel(x_col)
        plt.ylabel(y_col)
        plt.grid(True)
        plt.show()
