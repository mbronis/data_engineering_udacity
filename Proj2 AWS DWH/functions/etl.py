import configparser
import psycopg2
from functions.setup_aws import make_connection
from sql_queries import copy_table_queries, insert_table_queries

aws_config = 'configs/aws.cfg'


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    conn = make_connection(aws_config)
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()