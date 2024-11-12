import pytest
import pandas as pd
import json
from custom_tools.emission_data_average import EmissionDataTool_Average

# Sample mock data for testing
sample_data = pd.DataFrame({
    'Country or Area': ['Australia'] * 11,
    'Year': [2020, 2019, 2018, 2017, 2016, 2015, 2014, 2013, 2012, 2011, 2010],
    'sfc_emission': [90.90, 136.86, 145.26, 115.35, 117.12, 116.27, 105.63, 108.16, 114.98, 118.15, 129.80],
    'pfc_emission': [270.31, 303.14, 236.00, 202.62, 224.92, 171.32, 132.55, 192.00, 294.88, 301.30, 283.31],
    'green_house_emission': [528149.45, 546060.62, 552484.05, 550874.88, 543976.31, 534936.02, 526713.05, 532267.35, 541989.74, 538663.87, 538693.73]
})

@pytest.fixture
def tool_with_mock_data():
    return EmissionDataTool_Average(data=sample_data)

@pytest.mark.asyncio
async def test_average_emission(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country="Australia", emission_type="sfc_emission", trend_type="average")
    assert result["average_emission"] == pytest.approx(118.78, 0.01)  # Expected average for sfc_emission

@pytest.mark.asyncio
async def test_trend_last_x_years(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country="Australia", emission_type="pfc_emission", trend_type="last x years", num_years=3)
    data = result["data"]
    expected_years = [2020, 2019, 2018]
    assert [entry["Year"] for entry in data] == expected_years
    assert len(data) == 3

@pytest.mark.asyncio
async def test_trend_first_x_years(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country="Australia", emission_type="green_house_emission", trend_type="first x years", num_years=3)
    data = result["data"]
    expected_years = [2010, 2011, 2012]
    assert [entry["Year"] for entry in data] == expected_years
    assert len(data) == 3

@pytest.mark.asyncio
async def test_trend_recent_x_years(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country="Australia", emission_type="sfc_emission", trend_type="trend for x years", num_years=2)
    data = result["data"]
    expected_years = [2020, 2019]
    assert [entry["Year"] for entry in data] == expected_years
    assert len(data) == 2

@pytest.mark.asyncio
async def test_invalid_trend_type(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country="Australia", emission_type="sfc_emission", trend_type="unknown")
    assert "error" in result
    assert result["error"] == "Invalid trend type. Please choose 'average', 'last x years', 'first x years', or 'trend for x years'."

@pytest.mark.asyncio
async def test_missing_country(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country="NonExistentCountry", emission_type="sfc_emission", trend_type="average")
    assert "error" in result
    assert result["error"] == "No data available for the specified country: NonExistentCountry."

@pytest.mark.asyncio
async def test_missing_emission_type(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country="Australia", emission_type="non_existent_emission", trend_type="average")
    assert "error" in result
    assert result["error"] == 'Emission type "non_existent_emission" does not exist.'