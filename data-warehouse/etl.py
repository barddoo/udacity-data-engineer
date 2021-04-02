import configparser
import psycopg2
import psycopg2.extras
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur):
    """Get data from given s3 and put it into our Redshift cluster 
    """
    for query in copy_table_queries:
        cur.execute(query)


def insert_tables(cur):
    """Get data from tables came from s3 and put into our table schemas
    """
    for query in insert_table_queries:
        cur.execute(query)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()
    conn.autocommit = True

    load_staging_tables(cur)
    insert_tables(cur)

    conn.close()


if __name__ == "__main__":
    main()
