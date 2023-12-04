import os
import csv
import time
import logging
import psycopg2
import psycopg2.extras
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv

# DIRECTORY = os.path.dirname(os.path.abspath(__file__))

INPUT_FILE = 'raw_urls.csv'

FILE_PATH = f'./data/{INPUT_FILE}'

# Load environment variables from .env file
load_dotenv(dotenv_path=f'./.env')

# Configure logging
logging.basicConfig(level=logging.INFO)

# logging.info(f'path: {os.listdir(".")}')
# logging.info(f'data contents: {os.listdir("./data")}')


def parse_and_rename_url(url: str) -> dict:
    query_params = parse_qs(urlparse(url).query)
    renaming_map = {
        'a_bucket': 'ad_bucket',
        'a_type': 'ad_type',
        'a_source': 'ad_source',
        'a_v': 'schema_version',
        'a_g_campaignid': 'ad_campaign_id',
        'a_g_keyword': 'ad_keyword',
        'a_g_adgroupid': 'ad_group_id',
        'a_g_creative': 'ad_creative'
    }
    return {renaming_map.get(str(key)): query_params.get(key, [None])[0] for key in renaming_map}


def create_db_connection() -> psycopg2.connect:
    try:
        return psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
    except psycopg2.OperationalError as e:
        logging.error(f"Database connection failed: {e}")
        raise e


def insert_into_db(batch_data: list) -> int:
    conn = None
    cur = None
    rows_inserted = 0
    max_retries = 5
    retry_count = 1

    logging.info(f"DB_HOST': {os.getenv('DB_HOST')}")
    while retry_count <= max_retries:
        try:
            conn = create_db_connection()
            cur = conn.cursor()

            columns = ', '.join(batch_data[0].keys())
            values = [tuple(data.values()) for data in batch_data]
            query = f"TRUNCATE TABLE customer_visits; " \
                    f"INSERT INTO customer_visits ({columns}) VALUES %s"

            psycopg2.extras.execute_values(cur, query, values)
            conn.commit()
            rows_inserted = len(batch_data)
            logging.info(f"Inserted a batch of {rows_inserted} rows")
            break
        except psycopg2.OperationalError as e:
            logging.error(f"Database operation failed: {e}")
            logging.info(f"Retrying to connect/insert ({retry_count + 1}/{max_retries})...")
            time.sleep(5)
            retry_count += 1
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    if retry_count > max_retries:
        raise psycopg2.OperationalError("Failed to connect to PostgreSQL after several retries.")

    return rows_inserted


def process_csv_file(file_path: str) -> None:
    logging.info(f"Starting CSV parsing fro path: {file_path}")
    batch_data = []
    batch_size = 1000
    total_rows_inserted = 0
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            url = row['url']
            parsed_data = parse_and_rename_url(url)
            batch_data.append(parsed_data)
            if len(batch_data) >= batch_size:
                total_rows_inserted += insert_into_db(batch_data)
                batch_data = []
        if batch_data:
            total_rows_inserted += insert_into_db(batch_data)

    logging.info(f"Total rows inserted into database: {total_rows_inserted}")


def table_count():
    conn = None
    cur = None
    try:
        conn = create_db_connection()
        cur = conn.cursor()

        # Execute SQL query
        cur.execute("SELECT COUNT(*) as count FROM customer_visits;")
        result = cur.fetchone()

        # Log the count
        if result:
            count = result[0]
            logging.info(f"Row count in table customer_visits: {count}")
        else:
            logging.error("No rows found in table customer_visits.")

    except psycopg2.OperationalError as e:
        logging.error(f"Database operation failed: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    process_csv_file(FILE_PATH)
    table_count()
