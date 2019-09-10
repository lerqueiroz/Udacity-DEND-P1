import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """Delete Sparkify database if already exists and create Sparkify database.

    Returns:
        cur (cursor): Database connection object.
        conn (connection): Database connection object.

    """
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    # conn = psycopg2.connect("host=127.0.0.1 user=postgres password=superuser")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    # conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=superuser")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """Remove songplay table, user table, song table, artist table and time table on Sparkify database.

    Args:
        cur (cursor): Database connection object.
        conn (connection): Database connection object.

    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create songplay table, user table, song table, artist table and time table on Sparkify database.

    Args:
        cur (cursor): Database connection object.
        conn (connection): Database connection object.

    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()