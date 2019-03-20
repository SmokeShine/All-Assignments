import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import tqdm

def load_staging_tables(cur, conn):
    for query in tqdm.tqdm(copy_table_queries):
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in tqdm.tqdm(insert_table_queries):
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Loading Staging Tables")
    load_staging_tables(cur, conn)
    print("Inserting Data into Tables")
    insert_tables(cur, conn)
    print("Closing Connection")
    conn.close()


if __name__ == "__main__":
    main()