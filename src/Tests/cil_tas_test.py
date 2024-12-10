import sys
import os
import pytest
import pandas as pd

# Calculate the absolute path to the project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from custom_tools.cil_tas import CIL_TAS_Tool

# Mock data for testing
sample_data = pd.DataFrame({
    'name': [
        'Aruba', 'Aruba', 'UK', 'Aruba',
        'Afghanistan', 'Afghanistan', 'Kenya', 'Afghanistan'
    ],
    'ssp_range': [
        'SSP2-4.5', 'SSP2-4.5', 'SSP3-7.0', 'SSP3-7.0',
        'SSP2-4.5', 'SSP5-8.5', 'SSP5-8.5', 'SSP2-4.5'
    ],
    'year_range': [
        '1986-2005', '2020-2039', '2040-2059', '2080-2099',
        '1986-2005', '2020-2039', '2040-2059', '2080-2099'
    ],
    'p0.05': [79.42, 80.54, 81.23, 81.43, 52.22, 54.46, 55.48, 56.40],
    'p0.50': [79.62, 80.98, 81.60, 82.45, 52.77, 55.07, 56.51, 58.17],
    'p0.95': [79.76, 81.47, 82.20, 83.41, 53.23, 56.31, 57.67, 60.39]
})

@pytest.fixture
def tool_with_mock_data():
    """Fixture to initialize the PVDataTool with mock data."""
    tool = CIL_TAS_Tool(data=sample_data)
    return tool

@pytest.mark.asyncio
async def test_filter_by_name(tool_with_mock_data):
    """Test filtering by name, flag."""
    result = await tool_with_mock_data.run_impl(level = 'global', names=['Afghanistan'], flag = "jja")
    data = result['data']
    names_in_data = set(item['name'] for item in data)
    assert names_in_data == {'Afghanistan'}
    assert len(data) == 3

@pytest.mark.asyncio
async def test_filter_by_years(tool_with_mock_data):
    """Test filtering by years range, flag."""
    result = await tool_with_mock_data.run_impl(level = 'global', years_range=['2020-2039'], flag = "djf")
    data = result['data']
    names_in_data = set(item['name'] for item in data)
    assert names_in_data == {'Aruba', 'Afghanistan'}
    assert all(item['year_range'] == '2020-2039' for item in data)

@pytest.mark.asyncio
async def test_filter_by_ssp(tool_with_mock_data):
    """Test filtering by SSP range, flag."""
    result = await tool_with_mock_data.run_impl(level = 'global', ssp_range=['SSP5-8.5'], flag = "u32")
    data = result['data']
    names_in_data = set(item['name'] for item in data)
    assert names_in_data == {'Afghanistan', 'Kenya'}
    assert all(item['ssp_range'] == 'SSP5-8.5' for item in data)

@pytest.mark.asyncio
async def test_filter_by_percentiles(tool_with_mock_data):
    """Test filtering by Percentiles, flag."""
    result = await tool_with_mock_data.run_impl(level = 'global', percentiles=['0.50'], flag = "o95")
    data = result['data']
    names_in_data = set(item['name'] for item in data)
    assert names_in_data == {'Aruba', 'UK', 'Afghanistan', 'Kenya'}
    assert list(data[0].keys()) == ['name', 'ssp_range', 'year_range', 'p0.50']

@pytest.mark.asyncio
async def test_missing_name(tool_with_mock_data):
    """Test for missing name, no flag."""
    result = await tool_with_mock_data.run_impl(level = 'global', names=['NonExistentname'])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert "Data for the following isn't available: NonExistentname" in messages

@pytest.mark.asyncio
async def test_invalid_years(tool_with_mock_data):
    """Test for invalid year range."""
    result = await tool_with_mock_data.run_impl(level = 'global', years_range=['0000-1111'])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert "Data for the following year ranges isn't available: 0000-1111" in messages

@pytest.mark.asyncio
async def test_invalid_ssp(tool_with_mock_data):
    """Test for invalid ssp range."""
    result = await tool_with_mock_data.run_impl(level = 'global', ssp_range=['ssp99'])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert "Data for the following SSP ranges isn't available: ssp99" in messages

@pytest.mark.asyncio
async def test_invalid_yearrange(tool_with_mock_data):
    """Test for invalid quantile."""
    result = await tool_with_mock_data.run_impl(level = 'global', percentiles=['p12'])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert "Invalid percentile value(s): p12" in messages

@pytest.mark.asyncio
async def test_combined_filters(tool_with_mock_data):
    """Test combining filters."""
    result = await tool_with_mock_data.run_impl(level = 'global', names=['Afghanistan'], ssp_range=['SSP2-4.5'])
    data = result['data']
    assert len(data) > 0
    assert all(item['name'] == 'Afghanistan' for item in data)
    assert [item['year_range'] for item in data] == ['2080-2099', '1986-2005']
    assert [item['p0.50'] for item in data] == [58.17, 52.77]

@pytest.mark.asyncio
async def test_output_format(tool_with_mock_data):
    """Test output format with n results."""
    result = await tool_with_mock_data.run_impl(level = 'global', n=0)
    data = result['data']
    messages = result['messages']
    assert isinstance(data, list)
    assert isinstance(messages, list)
    if data:
        item = data[0]
        assert 'name' in item
        assert 'year_range' in item
        assert 'p0.95' in item
        assert len(data)==len(sample_data)

@pytest.mark.asyncio
async def test_no_filters(tool_with_mock_data):
    """Test default behavior with no filters."""
    result = await tool_with_mock_data.run_impl()
    data = result['data']
    messages = result['messages']
    assert data == None
    assert "Value error: Invalid level specified." in messages

@pytest.mark.asyncio
async def test_multiple_names_monthly(tool_with_mock_data):
    """Test filtering multiple names."""
    result = await tool_with_mock_data.run_impl(level = 'global', names=['Kenya', 'UK'])
    data = result['data']
    names_in_data = set(item['name'] for item in data)
    assert names_in_data == {'Kenya', 'UK'}
    assert len(data) == 2

@pytest.mark.asyncio
async def test_message_on_missing_data(tool_with_mock_data):
    """Test message on missing data."""
    result = await tool_with_mock_data.run_impl(level = 'global', names=['UK'], percentiles=['0.24'])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert "Invalid percentile value(s): 0.24" in messages

@pytest.mark.asyncio
async def test_groupby(tool_with_mock_data):
    """Test aggregation by mean grouped by SSP."""
    result = await tool_with_mock_data.run_impl(level = 'global', aggregate=['mean'], group_by=['year_range'])
    data = result['data']
    assert len(data) == 4
    if data:
        item = data[0]
        assert 'name' not in item
        assert 'year_range' in item
        assert 'ssp_range' not in item

@pytest.mark.asyncio
async def test_multiple_groupby(tool_with_mock_data):
    """Test aggregation by mean grouped by SSP."""
    result = await tool_with_mock_data.run_impl(level = 'global', aggregate=['mean'], group_by=['ssp_range', 'year_range'])
    data = result['data']
    assert len(data) == 7
    if data:
        item = data[0]
        assert 'name' not in item
        assert 'year_range' in item
        assert 'ssp_range' in item
    
@pytest.mark.asyncio
async def test_aggregation(tool_with_mock_data):
    """Test aggregation by Median."""
    result = await tool_with_mock_data.run_impl(level = 'global', aggregate=['median'])
    data = result['data']
    print(data)
    assert len(data) == 1
    assert [item['p0.95'] for item in data] == [sample_data['p0.95'].agg('median')]

@pytest.mark.asyncio
async def test_n(tool_with_mock_data):
    """Test filter for N values."""
    result = await tool_with_mock_data.run_impl(level = 'global', n=4)
    data = result['data']
    print(data)
    assert len(data) == 4
    assert [item['p0.95'] for item in data] == [83.41, 82.20, 81.47, 79.76]

@pytest.mark.asyncio
async def test_sort(tool_with_mock_data):
    """Test filter for all values witrh sorting."""
    result = await tool_with_mock_data.run_impl(level = 'global', sort="ascending", n=0)
    data = result['data']
    print(data)
    assert len(data) == len(sample_data)
    assert [item['p0.95'] for item in data][0] == 53.23

# Mock data for testing
sample_data_2 = pd.DataFrame({
    'name': [
        'Virginia', 'New York', 'Virginia', 'Texas'
    ],
    'ssp_range': [
        'SSP2-4.5', 'SSP2-4.5', 'SSP3-7.0', 'SSP5-8.5'
    ],
    'year_range': [
        '1986-2005', '2020-2039', '2040-2059', '2080-2099'
    ],
    'p0.05': [79.42, 80.54, 81.23, 81.43],
    'p0.50': [79.62, 80.98, 81.60, 82.45],
    'p0.95': [79.76, 81.47, 82.20, 83.41]
})

@pytest.fixture
def tool_with_mock_data_2():
    """Fixture to initialize the CIL_TAS_Tool with mock data."""
    tool = CIL_TAS_Tool(data=sample_data_2)
    return tool

@pytest.mark.asyncio
async def test_filter_by_level_usa(tool_with_mock_data_2):
    """Test filtering by level."""
    result = await tool_with_mock_data_2.run_impl(level = 'usa')
    data = result['data']
    names_in_data = set(item['name'] for item in data)
    assert names_in_data == {'Virginia', 'New York', 'Texas'}
    assert len(data) == 4

@pytest.mark.asyncio
async def test_filter_by_name_usa(tool_with_mock_data_2):
    """Test filtering by name, flag."""
    result = await tool_with_mock_data_2.run_impl(level = 'usa', names=['Virginia'], flag = "jja")
    data = result['data']
    names_in_data = set(item['name'] for item in data)
    assert names_in_data == {'Virginia'}
    assert len(data) == 2

@pytest.mark.asyncio
async def test_filter_by_years_usa(tool_with_mock_data_2):
    """Test filtering by years range, flag."""
    result = await tool_with_mock_data_2.run_impl(level = 'usa', years_range=['2020-2039'], flag = "djf")
    data = result['data']
    names_in_data = set(item['name'] for item in data)
    assert names_in_data == {'New York'}
    assert all(item['year_range'] == '2020-2039' for item in data)

@pytest.mark.asyncio
async def test_filter_by_ssp_usa(tool_with_mock_data_2):
    """Test filtering by SSP range, flag."""
    result = await tool_with_mock_data_2.run_impl(level = 'usa', ssp_range=['SSP5-8.5'], flag = "u32")
    data = result['data']
    names_in_data = set(item['name'] for item in data)
    assert names_in_data == {'Texas'}
    assert all(item['ssp_range'] == 'SSP5-8.5' for item in data)

@pytest.mark.asyncio
async def test_filter_by_percentiles_usa(tool_with_mock_data_2):
    """Test filtering by Percentiles, flag."""
    result = await tool_with_mock_data_2.run_impl(level = 'usa', percentiles=['0.50'], flag = "o95")
    data = result['data']
    names_in_data = set(item['name'] for item in data)
    assert names_in_data == {'Virginia', 'New York', 'Texas'}
    assert list(data[0].keys()) == ['name', 'ssp_range', 'year_range', 'p0.50']
