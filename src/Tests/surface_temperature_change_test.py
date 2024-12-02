import pytest
import pandas as pd
from unittest.mock import MagicMock
from custom_tools.surface_temperature_change import SurfaceTemperatureChangeTool

# Sample mock data for testing
sample_data = pd.DataFrame({
    'Country': ['India', 'Brazil', 'Australia'],
    '2000': [0.5, 0.6, 0.7],
    '2001': [0.7, 0.8, 0.9],
    '2002': [0.9, 1.0, 1.1],
    '2003': [1.0, 1.1, 1.2],
    '2004': [1.2, 1.3, 1.4]
})

@pytest.fixture
def tool_with_mock_data():
    return SurfaceTemperatureChangeTool(data=sample_data)

@pytest.mark.asyncio
async def test_get_name(tool_with_mock_data):
    result = tool_with_mock_data.get_name()
    assert result == "get_surface_temperature_change"

@pytest.mark.asyncio
async def test_get_description(tool_with_mock_data):
    result = tool_with_mock_data.get_description()
    assert "Retrieve surface temperature change data" in result

@pytest.mark.asyncio
async def test_get_params_definition(tool_with_mock_data):
    result = tool_with_mock_data.get_params_definition()
    assert "country" in result
    assert "start_year" in result
    assert "threshold" in result

@pytest.mark.asyncio
async def test_run_impl_with_country(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country="India")
    assert result["status"] == "success"
    assert len(result["data"]) > 0

@pytest.mark.asyncio
async def test_run_impl_with_threshold(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(threshold=1.0)
    assert result["status"] == "success"
    assert len(result["data"]) > 0

@pytest.mark.asyncio
async def test_run_impl_with_top_n(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(top_n=2)
    assert result["status"] == "success"
    assert len(result["data"]) == 2

@pytest.mark.asyncio
async def test_run_impl_with_top_n_empty_result(tool_with_mock_data):
    # Mock the `calculate_metric` method to return an empty DataFrame
    tool_with_mock_data.calculate_metric = MagicMock(return_value=pd.DataFrame())

    # Run the function with top_n set to 10 (even though we mock it as empty)
    result = await tool_with_mock_data.run_impl(top_n=10)
    
    # Assert that the status is 'error' and the expected message is returned
    assert result["status"] == "error"

@pytest.mark.asyncio
async def test_run_impl_with_decade(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(decade_start=2000)
    assert result["status"] == "success"
    assert len(result["data"]) > 0

@pytest.mark.asyncio
async def test_run_impl_with_compare_years(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(
        compare_years_difference=True, 
        start_year=2000, 
        end_year=2004, 
        difference_threshold=0.2
    )
    assert result["status"] == "success"
    assert len(result["data"]) > 0

@pytest.mark.asyncio
async def test_run_impl_with_invalid_country(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country="NonExistentCountry")
    assert result["status"] == "error"
    assert "No data available for the specified country(s)." in result["data"]

@pytest.mark.asyncio
async def test_run_impl_with_invalid_years(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(start_year=2025, end_year=2026)
    assert result["status"] == "error"
    assert "No valid years specified or years not found in the dataset." in result["data"]

@pytest.mark.asyncio
async def test_run_impl_with_invalid_threshold(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(threshold=10.0)
    assert result["status"] == "error"
    assert "No countries found exceeding the specified threshold." in result["data"]

@pytest.mark.asyncio
async def test_run_impl_with_empty_data(tool_with_mock_data):
    # Create an empty DataFrame
    empty_tool = SurfaceTemperatureChangeTool(data=pd.DataFrame())
    result = await empty_tool.run_impl(country="India")
    assert result["status"] == "error"
    assert "No data available for the specified parameters." in result["data"]

@pytest.mark.asyncio
async def test_calculate_metric(tool_with_mock_data):
    result = tool_with_mock_data.calculate_metric(sample_data, ["2000", "2001"], "average")
    assert "Metric" in result.columns
    assert result["Metric"].mean() == pytest.approx(0.7, 0.01)  # Expected average

@pytest.mark.asyncio
async def test_calculate_metric_with_sum(tool_with_mock_data):
    result = tool_with_mock_data.calculate_metric(sample_data, ["2000", "2001"], "sum")
    assert "Metric" in result.columns
    assert result["Metric"].sum() == pytest.approx(4.2, 0.01)  # Expected sum

@pytest.mark.asyncio
async def test_calculate_metric_with_sum(tool_with_mock_data):
    result = tool_with_mock_data.calculate_metric(sample_data, ["2000", "2001"], "difference")
    assert "Metric" in result.columns
    assert result["Metric"].sum() == pytest.approx(0.6, 0.01)  # Expected difference

@pytest.mark.asyncio
async def test_determine_years(tool_with_mock_data):
    years = tool_with_mock_data.determine_years(sample_data, start_year=2000, end_year=2004, interval=1)
    assert "2000" in years
    assert "2004" in years

@pytest.mark.asyncio
async def test_compare_years_with_difference(tool_with_mock_data):
    result = tool_with_mock_data.compare_years_with_difference(sample_data, 2000, 2004, threshold=0.2)
    assert result["status"] == "success"
    assert len(result["data"]) > 0

@pytest.mark.asyncio
async def test_compare_years_with_no_difference(tool_with_mock_data):
    result = tool_with_mock_data.compare_years_with_difference(sample_data, 2000, 2004, threshold=10.0)
    assert result["status"] == "error"
    assert "No countries found with temperature change difference exceeding the specified threshold." in result["data"]
