import psycopg2
from sql_queries import create_table_queries, drop_table_queries

password='abcd'

def create_database():
  """Recreates the 'sparkifydb', and returns a cursor and a database connection.
    
    Returns:
        cur (psycopg2.cursor): The database cursor
        conn (psycopg2.connection): The database connection """
  
  # connect to default database
  conn = psycopg2.connect(f"host=divn-test.cuvxpqcqzvyz.ap-northeast-1.rds.amazonaws.com dbname=DIVN-Master user=postgres password={password}")
  conn.set_session(autocommit=True)
  cur = conn.cursor()
    
 
  # close connection to default database
  conn.close()    
    
  # connect to sparkify database
  conn = psycopg2.connect(f"host=divn-test.cuvxpqcqzvyz.ap-northeast-1.rds.amazonaws.com dbname=sparkifydb user=postgres password={password}")
  cur = conn.cursor()
    
  return cur, conn


def drop_tables(cur, conn):
  """Drops all tables defined in `sql_queries.drop_table_queries`.
    
    Args:
        cur (psycopg2.cursor): A database cursor
        conn (psycopg2.connection): A database connection
    """
  
  for query in drop_table_queries:
      cur.execute(query)
      conn.commit()


def create_tables(cur, conn):
  """Creates all tables defined in `sql_queries.create_table_queries`.
    
    Args:
        cur (psycopg2.cursor): A database cursor
        conn (psycopg2.connection): A database connection
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
    main()w
