import psycopg2
import csv
from pathlib import Path
from io import StringIO
from sql_utils import *


def execute_query(conn, query):
    with conn.cursor() as cur:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(f"Error executing query {query}: {e}")
            conn.rollback()


def copy_csv_to_tables(conn, data_path):
    with conn.cursor() as cur:
        for item in data_path.iterdir():
            file_name = str(item).split("/")[-1].split(".")[0]
            table_name = (
                f"{file_name}_dim"
                if file_name != "transactions"
                else f"{file_name}_fct"
            )
            try:
                with open(item, "r") as f:
                    cur.copy_expert(f"copy {table_name} FROM STDIN WITH CSV HEADER", f)
                conn.commit()
            except Exception as e:
                print(f"Error copying data: {e}")
                conn.rollback()


def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    table_names = ["accounts_dim", "products_dim", "transactions_fct"]
    idx_cols = ["customer_id", "product_id", "transaction_id"]
    ct_queries = get_ct_queries()
    data_path = Path(__file__).parents[0] / "data"

    for ct_query, table_name, idx_col in zip(ct_queries, table_names, idx_cols):
        execute_query(conn=conn, query=get_dt_query(table_name))
        execute_query(conn=conn, query=ct_query)
        execute_query(
            conn=conn,
            query=get_index_query(
                table_name,
                column=idx_col,
            ),
        )
    copy_csv_to_tables(conn=conn, data_path=data_path)
    conn.close()


if __name__ == "__main__":
    main()
