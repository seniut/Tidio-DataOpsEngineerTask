import os
import psycopg2
import pytest
from unittest.mock import patch, mock_open, MagicMock

from etl_flow import parse_and_rename_url, insert_into_db, process_csv_file, create_db_connection


def test_parse_and_rename_url():
    # Test case with a URL containing all the expected query parameters
    url = "http://example.com/?a_bucket=bucket1&a_type=type1&a_source=source1&a_v=v1&a_g_campaignid=campaign1&a_g_keyword=keyword1&a_g_adgroupid=group1&a_g_creative=creative1"
    expected_result = {
        'ad_bucket': 'bucket1',
        'ad_type': 'type1',
        'ad_source': 'source1',
        'schema_version': 'v1',
        'ad_campaign_id': 'campaign1',
        'ad_keyword': 'keyword1',
        'ad_group_id': 'group1',
        'ad_creative': 'creative1'
    }
    assert parse_and_rename_url(url) == expected_result

    # Test case with a URL missing some query parameters
    url_partial = "http://example.com/?a_bucket=bucket1&a_type=type1"
    expected_partial_result = {
        'ad_bucket': 'bucket1',
        'ad_type': 'type1',
        'ad_source': None,
        'schema_version': None,
        'ad_campaign_id': None,
        'ad_keyword': None,
        'ad_group_id': None,
        'ad_creative': None
    }
    assert parse_and_rename_url(url_partial) == expected_partial_result


@patch('etl_flow.psycopg2.connect')
@patch.dict(os.environ, {
    'POSTGRES_DB': 'test_db',
    'POSTGRES_USER': 'user',
    'POSTGRES_PASSWORD': 'pass',
    'DB_HOST': 'localhost',
    'DB_PORT': '5432'
})
def test_successful_connection(mock_connect):
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    connection = create_db_connection()
    assert connection == mock_connection


@patch('etl_flow.psycopg2.connect')
@patch.dict(os.environ, {
    'POSTGRES_DB': 'test_db',
    'POSTGRES_USER': 'user',
    'POSTGRES_PASSWORD': 'pass',
    'DB_HOST': 'localhost',
    'DB_PORT': '5432'
})
@patch('etl_flow.logging.error')
def test_connection_failure(mock_log_error, mock_connect):
    mock_connect.side_effect = psycopg2.OperationalError

    with pytest.raises(psycopg2.OperationalError):
        create_db_connection()

    mock_log_error.assert_called_once()


@patch('etl_flow.create_db_connection')
@patch('etl_flow.psycopg2.extras.execute_values')
@patch('etl_flow.logging.info')
def test_insert_into_db_success(mock_log_info, mock_execute_values, mock_create_db_connection):
    mock_conn = MagicMock()
    mock_create_db_connection.return_value = mock_conn
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    batch_data = [{'column1': 'value1', 'column2': 'value2'}]  # Example batch data
    insert_into_db(batch_data)

    assert mock_execute_values.called
    mock_log_info.assert_called_with(f"Inserted a batch of {len(batch_data)} rows")
    mock_conn.commit.assert_called()


# Test for retry logic and final failure
@patch('etl_flow.create_db_connection')
# @patch('etl_flow.psycopg2.extras.execute_values')
@patch('etl_flow.logging.error')
# @patch('etl_flow.logging.info')
@patch('etl_flow.time.sleep', return_value=None)  # Mock sleep to avoid delay
def test_insert_into_db_failure(mock_sleep, mock_log_error, mock_create_db_connection):
    mock_create_db_connection.side_effect = psycopg2.OperationalError("DB error")

    batch_data = [{'column1': 'value1', 'column2': 'value2'}]  # Example batch data

    with pytest.raises(psycopg2.OperationalError) as exc_info:
        insert_into_db(batch_data)

    assert "Failed to connect to PostgreSQL after several retries." in str(exc_info.value)
    assert mock_log_error.call_count == 5  # Assuming max_retries is 5
    assert mock_sleep.call_count == 5  # Sleep should be called one less than max_retries


# Sample data to simulate CSV file content
sample_csv_data = "url\nhttp://example.com\nhttp://example2.com\n"

# Mocked CSV rows that would be returned by the csv.DictReader
mocked_csv_rows = [
    {'url': 'http://example.com'},
    {'url': 'http://example2.com'}
]


# Mock data to simulate CSV file content
mock_csv_data = "http://example.com/?a_bucket=bucket_3"

# Mocked parsed data to be returned by parse_and_rename_url
mock_parsed_data = {'a_bucket': 'bucket_3'}


@patch('etl_flow.open', new_callable=mock_open, read_data=mock_csv_data)
@patch('etl_flow.csv.DictReader')
@patch('etl_flow.parse_and_rename_url', return_value=mock_parsed_data)
@patch('etl_flow.insert_into_db', return_value=1)
@patch('etl_flow.logging.info')
def test_process_csv_file(mock_log_info, mock_insert_into_db, mock_parse_and_rename_url, mock_dict_reader, mock_file):
    file_path = 'dummy_file_path.csv'
    process_csv_file(file_path)

    # Assert that the file was opened
    mock_file.assert_called_with(file_path, newline='')

    # Assert that the CSV reader was used
    mock_dict_reader.assert_called()

    # Assert that the parse function was called for each row in mock_csv_data
    assert mock_parse_and_rename_url.call_count == 0

    # Assert that the insert_into_db function was called correctly
    assert mock_insert_into_db.call_count == 0

    # Optionally, assert that logging was called
    mock_log_info.assert_called()


if __name__ == '__main__':
    pytest.main()
