import pytest
import pandas as pd
import json
from custom_tools.rating_country import RatingCountryTool

# Sample mock data for testing
sample_data = pd.DataFrame({
    'Country': ['Argentina', 'Australia', 'Bhutan', 'Brazil', 'Canada', 'Chile', 'China', 'Costa Rica', 'Ethiopia', 'European Union'],
    'Overall rating': ['Critically insufficient', 'Insufficient', 'Almost sufficient', 'Insufficient', 'Insufficient', 'Insufficient', 'Highly insufficient', 'Almost sufficient', 'Almost sufficient', 'Insufficient'],
    'Policies and action': ['Highly insufficient', 'Insufficient', '1.5°C compatible', 'Insufficient', 'Insufficient', 'Almost sufficient', 'Insufficient', '1.5°C compatible', 'Critically insufficient', 'Insufficient'],
    'Domestic or supported target': ['Highly insufficient', 'Almost sufficient', '1.5°C global pathway', 'Almost sufficient', 'Almost sufficient', 'Almost sufficient', 'Highly insufficient', 'Almost sufficient', 'Critically insufficient', 'Insufficient'],
    'Fair share target': ['Critically insufficient', 'Insufficient', 'Almost sufficient', 'Almost sufficient', 'Insufficient', 'Insufficient', 'Insufficient', '1.5°C compatible', '1.5°C compatible', 'Insufficient'],
    'Climate finance': ['Not assessed', 'Critically insufficient', 'Not applicable', 'Not applicable', 'Highly insufficient', 'Not assessed', 'Not assessed', 'Not applicable', 'Not applicable', 'Insufficient'],
    'Net zero target': ['Poor', 'Poor', 'Information incomplete', 'Poor', 'Average', 'Acceptable', 'Poor', 'Acceptable', 'Information incomplete', 'Acceptable']
})

@pytest.fixture
def tool_with_mock_data():
    return RatingCountryTool(data=sample_data)

@pytest.mark.asyncio
async def test_filter_by_country(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['Argentina'])
    data = json.loads(result) if isinstance(result, str) else result
    assert all(item['Country'] == 'Argentina' for item in data)

@pytest.mark.asyncio
async def test_filter_by_component(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['Argentina'], Component=['Overall rating'])
    data = json.loads(result) if isinstance(result, str) else result
    assert all('Overall rating' in item for item in data)
    assert all('Policies and action' not in item for item in data)

@pytest.mark.asyncio
async def test_combined_filters(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['Argentina'], Component=['Overall rating', 'Net zero target'])
    data = json.loads(result) if isinstance(result, str) else result
    assert all(item['Country'] == 'Argentina' for item in data)
    assert all('Overall rating' in item and 'Net zero target' in item for item in data)

@pytest.mark.asyncio
async def test_missing_country(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['NonExistentCountry'])
    data = json.loads(result) if isinstance(result, str) else result
    assert data == []

@pytest.mark.asyncio
async def test_missing_component(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['Argentina'], Component=['NonExistentComponent'])
    data = json.loads(result) if isinstance(result, str) else result
    assert all('NonExistentComponent' not in item for item in data)

@pytest.mark.asyncio
async def test_all_countries(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country="all", Component=['Overall rating'])
    data = json.loads(result) if isinstance(result, str) else result
    countries = {item['Country'] for item in data}
    assert countries == set(sample_data['Country'].unique())

@pytest.mark.asyncio
async def test_all_components(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['Argentina'], Component="all")
    data = json.loads(result) if isinstance(result, str) else result
    assert all(column in data[0] for column in sample_data.columns if column != 'Country')

@pytest.mark.asyncio
async def test_all_countries_and_components(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country="all", Component="all")
    data = json.loads(result) if isinstance(result, str) else result
    countries = {item['Country'] for item in data}
    assert countries == set(sample_data['Country'].unique())
    assert all(column in data[0] for column in sample_data.columns)

@pytest.mark.asyncio
async def test_message_on_missing_data(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['NonExistentCountry'], Component=['NonExistentComponent'])
    data = json.loads(result) if isinstance(result, str) else result
    assert data == []

@pytest.mark.asyncio
async def test_aggregation(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(Country=['Argentina'], Component="all")
    data = json.loads(result) if isinstance(result, str) else result
    assert isinstance(data, list)
    assert all('Country' in item for item in data)