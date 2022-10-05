import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description:
        - loads the data from S3 to staging tables on Redshift.
    Arguments:
         cur: the cursor object.
         conn: connection to the database
    Returns:
         None
    """
    print("Inserting data from json files stored in S3 bucket into staging tables...")
    for query in copy_table_queries:
        print('Running' + query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description:
        - loads the data from staging tables to analytics (dimensional) tables on Redshift.
    Arguments:
        cur: the cursor object.
        conn: connection to the datase.
    Returns: 
        None
    """
    print('Inserting data from staging tables into dimensional tables...')
     
    for query in insert_table_queries:
        print(' Running ' + query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Description:
        - Extract songs metadata and user activity data from S3, transform it using a staging table, and load it into dimensional tables for analysis.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()