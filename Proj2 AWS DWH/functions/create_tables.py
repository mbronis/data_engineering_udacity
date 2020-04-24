import configparser
import psycopg2
from functions.setup_aws import make_connection
from functions.sql_queries import drop_table_queries, create_table_queries

aws_config = 'configs/aws.cfg'

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    conn = make_connection(aws_config)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()