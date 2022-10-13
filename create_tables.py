import psycopg2
from sql_quries import create_table_quries , drop_table_quries

def create_db():
    try:
        conn = psycopg2.connect("dbname = postgres user = postgres password=")
    except psycopg2.Error as e:
        print("cannot connect to database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("cannot get cursor")
        print(e)

    conn.set_session(autocommit=True)

    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e:
        print("cannot drop database")
        print(e)

    try:
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e:
        print("cannot create database")
        print(e)

    conn.close()

    try:
        conn = psycopg2.connect("dbname = sparkifydb user = postgres password=mostafa1208")
    except psycopg2.Error as e:
        print("cannot connect to database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("cannot get cursor")
        print(e)

    conn.set_session(autocommit=True)
    return cur , conn

def create_tables(cur):
    for create_query in create_table_quries:
        try:
            cur.execute(create_query)

        except psycopg2.Error as e:
            print(f"Cannot Create table: {create_query}")
            print(e)


def drop_tables(cur):
    for drop_query in drop_table_quries:
        try:
            cur.execute(drop_query)

        except psycopg2.Error as e:
            print(f"Cannot Drop table: {drop_query}")
            print(e)

def main():
    cur , conn = create_db()
    drop_tables(cur)
    create_tables(cur)

    cur.close
    conn.close()

if __name__ == '__main__':
    main()













