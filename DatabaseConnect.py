import pandas as pd
import psycopg2
import numpy as np
import psycopg2.extras as extras
from configparser import ConfigParser


def execute_values(conn, df, table, type):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    
    query_delete = "DELETE FROM " + table
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        if type == "OVERWRITE":
            cursor.execute(query_delete)
            extras.execute_values(cursor, query, tuples)
        elif type == "INSERT":
            extras.execute_values(cursor, query, tuples)

        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("The dataframe " + table +  " is inserted")
    cursor.close()


def create_query(conn, sql):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as erro:
        print("Error:", erro)
    print("The query has been run")
    cursor.close()


def config(filename='postgresql.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
        print(params)

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        
        # create a cursor
        cur = conn.cursor()
        
        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
        db_version = cur.fetchone()
        print(db_version)

        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)