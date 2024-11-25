import sys
import os
import pytest
import pandas as pd

# Calculate the absolute path to the project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from custom_tools.cil_global_tas import CIL_Global_TAS_Tool

# Mock data for testing
sample_data = pd.DataFrame({
    'country': [
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
    tool = CIL_Global_TAS_Tool(data=sample_data)
    return tool

@pytest.mark.asyncio
async def test_filter_by_country(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test filtering by country, flag."""
    result = await tool_with_mock_data.run_impl(countries=['Afghanistan'], flag = "jja")
    data = result['data']
    countries_in_data = set(item['country'] for item in data)
    assert countries_in_data == {'Afghanistan'}
    assert len(data) == 3

@pytest.mark.asyncio
async def test_filter_by_years(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test filtering by years range, flag."""
    result = await tool_with_mock_data.run_impl(years_range=['2020-2039'], flag = "djf")
    data = result['data']
    countries_in_data = set(item['country'] for item in data)
    assert countries_in_data == {'Aruba', 'Afghanistan'}
    assert all(item['year_range'] == '2020-2039' for item in data)

@pytest.mark.asyncio
async def test_filter_by_ssp(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test filtering by SSP range, flag."""
    result = await tool_with_mock_data.run_impl(ssp_range=['SSP5-8.5'], flag = "u32")
    data = result['data']
    countries_in_data = set(item['country'] for item in data)
    assert countries_in_data == {'Afghanistan', 'Kenya'}
    assert all(item['ssp_range'] == 'SSP5-8.5' for item in data)

@pytest.mark.asyncio
async def test_filter_by_percentiles(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test filtering by Percentiles, flag."""
    result = await tool_with_mock_data.run_impl(percentiles=['0.50'], flag = "o32")
    data = result['data']
    countries_in_data = set(item['country'] for item in data)
    assert countries_in_data == {'Aruba', 'UK', 'Afghanistan', 'Kenya'}
    assert list(data[0].keys()) == ['country', 'ssp_range', 'year_range', 'p0.50']

@pytest.mark.asyncio
async def test_missing_country(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test for missing country, no flag."""
    result = await tool_with_mock_data.run_impl(countries=['NonExistentCountry'])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert "Data for the following countries isn't available: NonExistentCountry" in messages

@pytest.mark.asyncio
async def test_invalid_years(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test for invalid year range."""
    result = await tool_with_mock_data.run_impl(years_range=['0000-1111'])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert "Data for the following year ranges isn't available: 0000-1111" in messages

@pytest.mark.asyncio
async def test_invalid_ssp(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test for invalid ssp range."""
    result = await tool_with_mock_data.run_impl(ssp_range=['ssp99'])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert "Data for the following SSP ranges isn't available: ssp99" in messages

@pytest.mark.asyncio
async def test_invalid_yearrange(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test for invalid quantile."""
    result = await tool_with_mock_data.run_impl(percentiles=['p12'])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert "Invalid percentile value(s): p12" in messages

@pytest.mark.asyncio
async def test_combined_filters(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test combining filters."""
    result = await tool_with_mock_data.run_impl(countries=['Afghanistan'], ssp_range=['SSP2-4.5'])
    data = result['data']
    assert len(data) > 0
    assert all(item['country'] == 'Afghanistan' for item in data)
    assert [item['year_range'] for item in data] == ['2080-2099', '1986-2005']
    assert [item['p0.50'] for item in data] == [58.17, 52.77]

@pytest.mark.asyncio
async def test_output_format(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test output format with n results."""
    result = await tool_with_mock_data.run_impl(n=0)
    data = result['data']
    messages = result['messages']
    assert isinstance(data, list)
    assert isinstance(messages, list)
    if data:
        item = data[0]
        assert 'country' in item
        assert 'year_range' in item
        assert 'p0.95' in item
        assert len(data)==len(sample_data)

@pytest.mark.asyncio
async def test_no_filters(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test default behavior with no filters."""
    result = await tool_with_mock_data.run_impl()
    data = result['data']
    assert len(data) == len(sample_data)
    countries_in_data = set(item['country'] for item in data)
    assert countries_in_data == {'Aruba', 'UK', 'Afghanistan', 'Kenya'}

@pytest.mark.asyncio
async def test_multiple_countries_monthly(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test filtering multiple countries."""
    result = await tool_with_mock_data.run_impl(countries=['Kenya', 'UK'])
    data = result['data']
    countries_in_data = set(item['country'] for item in data)
    assert countries_in_data == {'Kenya', 'UK'}
    assert len(data) == 2

@pytest.mark.asyncio
async def test_message_on_missing_data(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test message on missing data."""
    result = await tool_with_mock_data.run_impl(countries=['UK'], percentiles=['0.24'])
    data = result['data']
    messages = result['messages']
    assert len(data) == 0
    assert "Invalid percentile value(s): 0.24" in messages

@pytest.mark.asyncio
async def test_groupby(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test aggregation by mean grouped by SSP."""
    result = await tool_with_mock_data.run_impl(aggregate=['mean'], group_by=['year_range'])
    data = result['data']
    assert len(data) == 4
    if data:
        item = data[0]
        assert 'country' not in item
        assert 'year_range' in item
        assert 'ssp_range' not in item

@pytest.mark.asyncio
async def test_multiple_groupby(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test aggregation by mean grouped by SSP."""
    result = await tool_with_mock_data.run_impl(aggregate=['mean'], group_by=['ssp_range', 'year_range'])
    data = result['data']
    assert len(data) == 7
    if data:
        item = data[0]
        assert 'country' not in item
        assert 'year_range' in item
        assert 'ssp_range' in item
    
@pytest.mark.asyncio
async def test_aggregation(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test aggregation by Median."""
    result = await tool_with_mock_data.run_impl(aggregate=['median'])
    data = result['data']
    print(data)
    assert len(data) == 1
    assert [item['p0.95'] for item in data] == sample_data['p0.95'].agg('median')

@pytest.mark.asyncio
async def test_n(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test filter for N values."""
    result = await tool_with_mock_data.run_impl(n=4)
    data = result['data']
    print(data)
    assert len(data) == 4
    assert [item['p0.95'] for item in data] == [83.41, 82.20, 81.47, 79.76]

@pytest.mark.asyncio
async def test_sort(tool_with_mock_data: CIL_Global_TAS_Tool):
    """Test filter for all values witrh sorting."""
    result = await tool_with_mock_data.run_impl(sort="ascending", n=0)
    data = result['data']
    print(data)
    assert len(data) == len(sample_data)
    assert [item['p0.95'] for item in data][0] == 53.23