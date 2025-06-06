from psycopg2.pool import SimpleConnectionPool

# PostgreSQL connection string (DSN)
PG_DSN = (
    "postgresql://neondb_owner:npg_SfzAVOih23Xp"
    "@ep-fancy-sunset-a1xqv7sq-pooler.ap-southeast-1.aws.neon.tech"
    "/jobgenai?sslmode=require"
)

# Singleton pool using DSN
pool = SimpleConnectionPool(
    minconn=1,
    maxconn=5,
    dsn=PG_DSN
)

def get_conn():
    return pool.getconn()

def put_conn(conn):
    pool.putconn(conn)

def close_all():
    pool.closeall()
