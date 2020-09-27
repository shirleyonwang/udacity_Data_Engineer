import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function can be used to copy files in S3 to staging_events and staging_songs.

    Arguments:
        cur: the cursor object. 
        conn: connection to database.  

    Returns:
        None
    """
    for query in copy_table_queries:
        print('1111111111111111')            
        cur.execute(query)
        conn.commit()
              

def insert_tables(cur, conn):
    """
    Description: This function can be used to insert data to 5 tables from staging_events and staging_songs.

    Arguments:
        cur: the cursor object. 
        conn: connection to database. 

    Returns:
        None
    """    
    for query in insert_table_queries:
        print('22222222222')    
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()