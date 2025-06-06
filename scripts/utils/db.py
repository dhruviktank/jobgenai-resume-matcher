from psycopg2.pool import ThreadedConnectionPool
import os

PG_DSN = os.getenv("PG_DSN") or (
    "postgresql://neondb_owner:npg_SfzAVOih23Xp"
    "@ep-fancy-sunset-a1xqv7sq-pooler.ap-southeast-1.aws.neon.tech"
    "/jobgenai?sslmode=require"
)

_pool = None

def init_pool():
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(minconn=1, maxconn=5, dsn=PG_DSN)

def get_conn():
    if _pool is None:
        raise RuntimeError("Connection pool not initialized")
    return _pool.getconn()

def put_conn(conn):
    if _pool:
        _pool.putconn(conn)

def close_all():
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None
