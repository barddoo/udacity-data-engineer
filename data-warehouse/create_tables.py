import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur):
    for query in drop_table_queries:
        cur.execute(query)


def create_tables(cur):
    for query in create_table_queries:
        cur.execute(query)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()
    conn.autocommit = True

    drop_tables(cur)
    create_tables(cur)

    conn.close()


if __name__ == "__main__":
    main()
