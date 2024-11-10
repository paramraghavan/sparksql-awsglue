import snowflake.connector
import pandas as pd
import os
from datetime import datetime
import logging
from typing import List, Dict


class SnowflakeExporter:
    def __init__(self, conn_params: Dict):
        """Initialize Snowflake connection parameters"""
        self.conn_params = conn_params
        self.setup_logging()

    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename=f'snowflake_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        )

    def get_chunk_query(self, table_name: str, offset: int, chunk_size: int) -> str:
        """Generate query for each chunk"""
        return f"""
            SELECT *
            FROM {table_name}
            LIMIT {chunk_size}
            OFFSET {offset}
        """

    def export_table(self, table_name: str, output_path: str, chunk_size: int = 1000000):
        """Export a single table in chunks"""
        try:
            conn = snowflake.connector.connect(**self.conn_params)
            cursor = conn.cursor()

            # Get total count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cursor.fetchone()[0]

            logging.info(f"Starting export of {table_name} - Total rows: {total_rows}")

            # Create output directory if it doesn't exist
            os.makedirs(output_path, exist_ok=True)

            # Export in chunks
            for offset in range(0, total_rows, chunk_size):
                chunk_number = offset // chunk_size + 1
                chunk_file = f"{output_path}/{table_name}_chunk_{chunk_number}.csv"

                if os.path.exists(chunk_file):
                    logging.info(f"Chunk {chunk_number} already exists, skipping...")
                    continue

                logging.info(f"Exporting chunk {chunk_number} ({offset} to {offset + chunk_size})")

                query = self.get_chunk_query(table_name, offset, chunk_size)
                df = pd.read_sql(query, conn)

                # Write chunk to CSV with compression
                df.to_csv(chunk_file, index=False, compression='gzip')

                logging.info(f"Completed chunk {chunk_number}")

        except Exception as e:
            logging.error(f"Error exporting {table_name}: {str(e)}")
            raise
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()

    def export_multiple_tables(self, table_list: List[str], base_output_path: str):
        """Export multiple tables"""
        for table in table_list:
            output_path = os.path.join(base_output_path, table)
            logging.info(f"Starting export of table: {table}")
            self.export_table(table, output_path)
            logging.info(f"Completed export of table: {table}")


# Example usage
if __name__ == "__main__":
    conn_params = {
        'user': 'your_username',
        'password': 'your_password',
        'account': 'your_account',
        'warehouse': 'your_warehouse',
        'database': 'your_database',
        'schema': 'your_schema'
    }

    tables_to_export = [
        'TABLE1',
        'TABLE2',
        'TABLE3'
    ]

    exporter = SnowflakeExporter(conn_params)
    exporter.export_multiple_tables(tables_to_export, './data_export')
