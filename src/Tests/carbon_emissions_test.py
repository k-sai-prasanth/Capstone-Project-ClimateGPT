import sys
import os
import pytest
import pandas as pd

# Calculate the absolute path to the project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from custom_tools.carbon_emissions_tool import CarbonEmissionDataTool

# Mock data for testing
sample_data = pd.DataFrame({
    'country': ['India', 'India', 'Brazil', 'Germany', 'United States'],
    'sector': ['Residential', 'Power', 'Industry', 'Residential', 'Power'],
    'date': ['01/01/2023', '02/01/2023', '01/02/2023', '01/01/2024', '03/01/2023'],
    'MtCO2 per day': [1.0, 2.0, 3.0, 4.0, 5.0],
})

@pytest.fixture
def tool_with_mock_data():
    # Instantiate the tool with sample_data injected
    tool = CarbonEmissionDataTool(data=sample_data)
    return tool

@pytest.mark.asyncio
async def test_filter_by_country(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(countries=['India'])
    data = result['data']
    countries_in_data = set(item['Country'] for item in data)
    assert countries_in_data == {'India'}
    assert len(data) > 0

@pytest.mark.asyncio
async def test_filter_by_sector(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(sectors=['Power'])
    data = result['data']
    sectors_in_data = set(item['Sector'] for item in data)
    assert sectors_in_data == {'Power'}
    assert len(data) > 0

@pytest.mark.asyncio
async def test_filter_by_date(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(dates=['02/01/2023'])
    data = result['data']
    expected_countries = {'India'}
    countries_in_data = set(item['Country'] for item in data)
    assert expected_countries.issubset(countries_in_data)

@pytest.mark.asyncio
async def test_filter_by_year(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(years=[2023])
    data = result['data']
    expected_countries = {'India', 'Brazil', 'United States'}
    countries_in_data = set(item['Country'] for item in data)
    assert expected_countries.issubset(countries_in_data)

@pytest.mark.asyncio
async def test_missing_country(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(countries=['NonExistentCountry'])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert "Data for the following countries isn't available: NonExistentCountry" in messages[0]

@pytest.mark.asyncio
async def test_missing_sector(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(sectors=['NonExistentSector'])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert "Data for the following sectors in these countries isn't available: NonExistentSector" in messages[0]

@pytest.mark.asyncio
async def test_combined_filters(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(countries=['India'], sectors=['Residential'], years=[2023])
    data = result['data']
    assert len(data) == 1
    assert data[0]['Country'] == 'India'
    assert data[0]['Sector'] == 'Residential'
    assert pytest.approx(data[0]['Emissions'], 0.1) == 1.0  # Allow small float variance

@pytest.mark.asyncio
async def test_output_format(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl()
    data = result['data']
    messages = result['messages']
    assert isinstance(data, list)
    assert isinstance(messages, list)
    if data:
        item = data[0]
        assert 'Country' in item
        assert 'Sector' in item
        assert 'Emissions' in item

@pytest.mark.asyncio
async def test_no_filters(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl()
    data = result['data']
    assert len(data) > 0
    # Check that data includes the expected sample countries
    expected_countries = {'India', 'Brazil', 'Germany', 'United States'}
    countries_in_data = set(item['Country'] for item in data)
    assert expected_countries.issubset(countries_in_data)

@pytest.mark.asyncio
async def test_invalid_date_format(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(dates=['2023/01/01'])
    data = result['data']
    assert len(data) == 0

@pytest.mark.asyncio
async def test_multiple_countries_sectors(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(countries=['India', 'Brazil'], sectors=['Residential', 'Industry'])
    data = result['data']
    countries_in_data = set(item['Country'] for item in data)
    sectors_in_data = set(item['Sector'] for item in data)
    assert {'India', 'Brazil'}.issubset(countries_in_data)
    assert {'Residential', 'Industry'}.issubset(sectors_in_data)

@pytest.mark.asyncio
async def test_message_on_missing_data(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(countries=['Germany'], sectors=['NonExistentSector'])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert "Data for the following sectors in these countries isn't available: NonExistentSector" in messages[0]

@pytest.mark.asyncio
async def test_aggregation(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(years=[2023])
    data = result['data']
    total_emissions = sum(item['Emissions'] for item in data)
    assert pytest.approx(total_emissions, 0.1) == 11.0  # Allow small float variance
