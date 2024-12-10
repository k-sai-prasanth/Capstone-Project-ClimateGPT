import sys
import os
import pytest
import pandas as pd

# Calculate the absolute path to the project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from src.custom_tools.ren_ninja import RenNinjaTool

# Mock data for testing
sample_data = pd.DataFrame({
    'time': pd.to_datetime(['2023-01-01 23:00:00', '1999-10-27 06:00:00', '2023-01-01 02:00:00', 
                            '2000-05-05 01:00:00', '2000-05-05 14:00:00']),
    'GB': [0.0042, 0.0923, 0.0011, 0.1942, 0.1324], 
    'US':  [0.3452, 0.4253, 0.0421, 0.5294, 0.6532],
    'IN': [0.7029, 0.8569, 0.2385, 0.9241, 0.1245]
})

@pytest.fixture
def tool_with_mock_data():
    """Fixture to initialize the RenNinjaTool with mock data."""
    tool = RenNinjaTool(data=sample_data)
    return tool

@pytest.mark.asyncio
async def test_filter_by_country(tool_with_mock_data):
    """Test filtering by country."""
    result = await tool_with_mock_data.run_impl(level = 'pv', countries=['India'])
    data = result['data']
    countries_in_data = set(item['country'] for item in data)
    assert countries_in_data == {'India'}
    assert len(data) > 0

@pytest.mark.asyncio
async def test_filter_by_time(tool_with_mock_data):
    """Test filtering by date and hour."""
    result = await tool_with_mock_data.run_impl(level = 'pv', dates=['2023-01-01'], hours=[23])
    data = result['data']
    countries_in_data = set(item['country'] for item in data)
    assert countries_in_data == {'United Kingdom', 'India', 'United States'}
    assert all(item['time'].hour == 23 for item in data)

@pytest.mark.asyncio
async def test_filter_by_year(tool_with_mock_data):
    """Test filtering by year."""
    result = await tool_with_mock_data.run_impl(level = 'pv', years=[2023])
    data = result['data']
    years_in_data = set(pd.to_datetime(item['time']).year for item in data)
    assert years_in_data == {2023}

@pytest.mark.asyncio
async def test_missing_country(tool_with_mock_data):
    """Test for missing country."""
    result = await tool_with_mock_data.run_impl(level = 'pv', countries=['NonExistentCountry'])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert "Data for the following countries isn't available: NonExistentCountry" in messages

@pytest.mark.asyncio
async def test_combined_filters(tool_with_mock_data):
    """Test combining filters."""
    result = await tool_with_mock_data.run_impl(level = 'pv', countries=['United Kingdom'], years=[2023], hours=[23])
    data = result['data']
    assert len(data) > 0
    assert all(item['country'] == 'United Kingdom' for item in data)
    assert all(pd.to_datetime(item['time']).year == 2023 for item in data)
    assert all(pd.to_datetime(item['time']).hour == 23 for item in data)

@pytest.mark.asyncio
async def test_output_format(tool_with_mock_data):
    """Test output format."""
    result = await tool_with_mock_data.run_impl(level = 'pv')
    data = result['data']
    messages = result['messages']
    assert isinstance(data, list)
    assert isinstance(messages, list)
    if data:
        item = data[0]
        assert 'country' in item
        assert 'time' in item
        assert 'capacity_factor' in item

@pytest.mark.asyncio
async def test_no_filters(tool_with_mock_data):
    """Test default behavior with no filters."""
    result = await tool_with_mock_data.run_impl()
    data = result['data']
    messages = result['messages']
    assert data == None
    assert "Value error: Invalid level specified." in messages

@pytest.mark.asyncio
async def test_invalid_date_format(tool_with_mock_data):
    """Test invalid date format."""
    result = await tool_with_mock_data.run_impl(level = 'pv', dates=['5/5/00'])
    data = result['data']
    pv_in_data = set(item['capacity_factor'] for item in data)
    assert pv_in_data == {0.1324, 0.1245, 0.1942, 0.5294, 0.6532, 0.9241}

@pytest.mark.asyncio
async def test_multiple_countries_monthly(tool_with_mock_data):
    """Test filtering multiple countries and aggregating monthly."""
    result = await tool_with_mock_data.run_impl(level = 'pv', countries=['United Kingdom', 'United States'], data_merge='monthly')
    data = result['data']
    countries_in_data = set(item['country'] for item in data)
    assert {'United Kingdom', 'United States'}.issubset(countries_in_data)
    assert all('time' in item for item in data)

@pytest.mark.asyncio
async def test_message_on_missing_dates(tool_with_mock_data):
    """Test message on missing data."""
    result = await tool_with_mock_data.run_impl(level = 'pv', countries=['United States'], dates=['2027-02-12'])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert "Data for the following dates isn't available: 2027-02-12" in messages

@pytest.mark.asyncio
async def test_message_on_missing_years(tool_with_mock_data):
    """Test message on missing data."""
    result = await tool_with_mock_data.run_impl(level = 'pv', countries=['United States'], years=[2030])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert "Data for the following years isn't available: 2030" in messages

@pytest.mark.asyncio
async def test_message_on_incorrect_months(tool_with_mock_data):
    """Test message on missing data."""
    result = await tool_with_mock_data.run_impl(level = 'pv', countries=['United States'], months=[20,12])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert all(item['time'].month == 12 for item in data) 
    assert "Incorrect months: 20" in messages

@pytest.mark.asyncio
async def test_message_on_incorrect_hours(tool_with_mock_data):
    """Test message on missing data."""
    result = await tool_with_mock_data.run_impl(level = 'pv', countries=['United States'], hours=[23,35])
    data = result['data']
    messages = result['messages']
    assert len(data) != 0
    assert all(item['time'].hour == 23 for item in data) 
    assert "Incorrect hours: 35" in messages

@pytest.mark.asyncio
async def test_aggregation(tool_with_mock_data):
    """Test aggregation by yearly mean grouped by time."""
    result = await tool_with_mock_data.run_impl(level = 'pv', years=[2023], data_merge='yearly', aggregate=['mean'], group_by='time')
    data = result['data']
    assert len(data) > 0
    assert all(isinstance(item['capacity_factor'], float) for item in data)
