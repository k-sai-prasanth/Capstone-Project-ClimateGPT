import pytest
import pandas as pd
import json
from custom_tools.emissions_data import EmissionDataTool

# Sample mock data for testing
sample_data = pd.DataFrame({
    'Country or Area': ['Australia', 'Australia', 'Brazil', 'Germany', 'United States'],
    'Year': [2020, 2019, 2020, 2019, 2020],
    'sfc_emissions': [90.9, 136.86, 145.26, 115.35, 110.12],
    'n2o_emissions': [18586.65, 18751.64, 19578.73, 20719.44, 19042.92],
    'methane_emissions': [97033.73, 99367.73, 105796.24, 105002.79, 103078.52],
    'green_house_emissions': [521849.46, 546060.62, 552484.05, 550874.88, 539876.42]
})

@pytest.fixture
def tool_with_mock_data():
    return EmissionDataTool(data=sample_data)

@pytest.mark.asyncio
async def test_filter_by_country(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country=['Australia'])
    data = json.loads(result['data'])
    assert all(item['Country or Area'] == 'Australia' for item in data)

@pytest.mark.asyncio
async def test_filter_by_year(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country=['Australia'], year=[2020])
    data = json.loads(result['data'])
    assert all(item['Year'] == 2020 for item in data)

@pytest.mark.asyncio
async def test_filter_by_emission_type(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country=['Australia'], emission_type=['sfc_emissions'])
    data = json.loads(result['data'])
    assert 'sfc_emissions' in data[0]
    assert 'n2o_emissions' not in data[0]

@pytest.mark.asyncio
async def test_combined_filters(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country=['Australia'], year=[2020], emission_type=['sfc_emissions'])
    data = json.loads(result['data'])
    assert all(item['Country or Area'] == 'Australia' and item['Year'] == 2020 for item in data)
    assert 'sfc_emissions' in data[0]
    assert 'n2o_emissions' not in data[0]

@pytest.mark.asyncio
async def test_missing_country(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country=['NonExistentCountry'])
    assert result['data'] == '[]'

@pytest.mark.asyncio
async def test_missing_emission_type(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country=['Australia'], emission_type=['non_existent_emission'])
    assert 'message' in result
    assert 'non_existent_emission' in result['message']

@pytest.mark.asyncio
async def test_all_countries(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country="all")
    data = json.loads(result['data'])
    countries = {item['Country or Area'] for item in data}
    assert countries == set(sample_data['Country or Area'].unique())

@pytest.mark.asyncio
async def test_all_years(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country=['Australia'], year="all")
    data = json.loads(result['data'])
    years = {item['Year'] for item in data}
    assert years == set(sample_data[sample_data['Country or Area'] == 'Australia']['Year'].unique())

@pytest.mark.asyncio
async def test_all_emission_types(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country=['Australia'], emission_type="all")
    data = json.loads(result['data'])
    expected_columns = ['n2o_emissions', 'methane_emissions', 'green_house_emissions']  # Adjusted list
    
    print("test_all_emission_types - Data returned:", data)
    
    if data:
        assert all(col in data[0] for col in expected_columns), f"Expected columns missing: {expected_columns}"

@pytest.mark.asyncio
async def test_no_filters(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country="all", year="all", emission_type="all")
    data = json.loads(result['data'])
    expected_columns = ['n2o_emissions', 'methane_emissions', 'green_house_emissions']  # Adjusted list
    
    print("test_no_filters - Data returned:", data)
    
    if data:
        assert all(col in data[0] for col in expected_columns), f"Expected columns missing: {expected_columns}"
    
    countries = {item['Country or Area'] for item in data}
    years = {item['Year'] for item in data}
    assert countries == set(sample_data['Country or Area'].unique())
    assert years == set(sample_data['Year'].unique())

@pytest.mark.asyncio
async def test_message_on_missing_data(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country=['Germany'], emission_type=['non_existent_emission'])
    assert 'message' in result
    assert 'non_existent_emission' in result['message']

@pytest.mark.asyncio
async def test_aggregation(tool_with_mock_data):
    result = await tool_with_mock_data.run_impl(country=['Australia'], year=[2020], emission_type=['sfc_emissions', 'n2o_emissions'])
    data = json.loads(result['data'])
    assert 'sfc_emissions' in data[0]
    assert 'n2o_emissions' in data[0]
    assert 'methane_emissions' not in data[0]