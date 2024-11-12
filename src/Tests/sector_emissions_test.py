import pytest
import pandas as pd
import json
from custom_tools.sector_emission import SectorEmissionTool

# Sample mock data for testing
sample_data = pd.DataFrame({
    'Sector': ['Buildings', 'Buildings', 'Buildings', 'Buildings', 'Buildings', 'Buildings', 'Buildings', 'Buildings'],
    'Description': [
        'Buildings emissions intensity (per floor area, commercial)',
        'Buildings emissions intensity (per floor area, residential)',
        'Buildings energy intensity (commercial)',
        'Buildings energy intensity (residential)',
        'Emissions intensity of electricity generation',
        'Share of coal in electricity generation',
        'Cement emissions intensity (per product)',
        'Steel emissions intensity (per product)'
    ],
    'Country': ['Australia', 'Australia', 'Canada', 'Canada', 'Chile', 'Chile', 'Indonesia', 'Indonesia'],
    'Year': [2005, 2010, 2005, 2010, 2005, 2010, 2005, 2010],
    'Emission Value': [149.73, 141.68, 115.71, 90.62, 98.48, 147.86, 83.10, 137.54],
    'Unit': ['kg CO2 / m2'] * 8
})

@pytest.fixture
def tool_with_mock_data():
    return SectorEmissionTool(data=sample_data)

@pytest.mark.asyncio
async def test_filter_by_country(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['Australia'])
    data = json.loads(result) if isinstance(result, str) else result
    assert all(item['Country'] == 'Australia' for item in data)

@pytest.mark.asyncio
async def test_filter_by_sector(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['Australia'], Sector=['Buildings'])
    data = json.loads(result) if isinstance(result, str) else result
    assert all(item['Sector'] == 'Buildings' for item in data)

@pytest.mark.asyncio
async def test_filter_by_year(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['Australia'], Year=[2005])
    data = json.loads(result) if isinstance(result, str) else result
    assert all(item['Year'] == 2005 for item in data)

@pytest.mark.asyncio
async def test_filter_by_description(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['Australia'], Description=['Buildings emissions intensity (per floor area, commercial)'])
    data = json.loads(result) if isinstance(result, str) else result
    assert all(item['Description'] == 'Buildings emissions intensity (per floor area, commercial)' for item in data)

@pytest.mark.asyncio
async def test_combined_filters(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['Australia'], Sector=['Buildings'], Year=[2005], Description=['Buildings emissions intensity (per floor area, commercial)'])
    data = json.loads(result) if isinstance(result, str) else result
    assert all(item['Country'] == 'Australia' and item['Sector'] == 'Buildings' and item['Year'] == 2005 for item in data)

@pytest.mark.asyncio
async def test_all_countries(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country="all", Sector=['Buildings'])
    data = json.loads(result) if isinstance(result, str) else result
    countries = {item['Country'] for item in data}
    assert countries == set(sample_data['Country'].unique())

@pytest.mark.asyncio
async def test_all_sectors(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['Australia'], Sector="all")
    data = json.loads(result) if isinstance(result, str) else result
    assert all(item['Sector'] in sample_data['Sector'].unique() for item in data)

@pytest.mark.asyncio
async def test_average_emission_all_years(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['Australia'], Year="all", Sector="all", Description=None)
    data = json.loads(result) if isinstance(result, str) else result
    for item in data:
        assert 'Average Emission Value' in item

@pytest.mark.asyncio
async def test_average_emission_with_description(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['Australia'], Year="all", Sector="all", Description="Buildings emissions intensity (per floor area, commercial)")
    data = json.loads(result) if isinstance(result, str) else result
    assert all('Average Emission Value' in item for item in data)

@pytest.mark.asyncio
async def test_missing_country(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['NonExistentCountry'])
    data = json.loads(result) if isinstance(result, str) else result
    assert data == []

@pytest.mark.asyncio
async def test_missing_sector(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['Australia'], Sector=['NonExistentSector'])
    data = json.loads(result) if isinstance(result, str) else result
    assert data == []