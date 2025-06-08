from psycopg2.pool import ThreadedConnectionPool
import psycopg2
import logging

# PostgreSQL connection string (DSN)
PG_DSN = (
    "postgresql://neondb_owner:npg_SfzAVOih23Xp"
    "@ep-fancy-sunset-a1xqv7sq-pooler.ap-southeast-1.aws.neon.tech"
    "/jobgenai?sslmode=require"
)

# Singleton pool using DSN
pool = ThreadedConnectionPool(
    minconn=1,
    maxconn=5,
    dsn=PG_DSN
)

def validate_connection(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True
    except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
        logging.warning(f"Connection validation failed: {e}")
        return False

def get_conn():
    conn = pool.getconn()
    if not validate_connection(conn):
        try:
            conn.close()
        except Exception:
            pass
        conn = pool.getconn()
        if not validate_connection(conn):
            raise psycopg2.OperationalError("Could not establish valid DB connection")
    return conn

def put_conn(conn):
    pool.putconn(conn)

def close_all():
    pool.closeall()
