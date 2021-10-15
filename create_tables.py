import psycopg2
from sql_queries import create_table_queries, drop_table_queries

password='abcd'

def create_database():
  
    
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
  
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
 
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