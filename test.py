
import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool

# 初始化数据库连接池
DATABASE_HOST='49.234.214.138'
DATABASE_PORT=5432
DATABASE_USERNAME='postgres'
DATABASE_PASSWORD='zhang002508'
DATABASE_NAME='postgres'

connPool = SimpleConnectionPool(10, 50,
        host=DATABASE_HOST,
        port=DATABASE_PORT,
        user=DATABASE_USERNAME,
        password=DATABASE_PASSWORD,
        database=DATABASE_NAME)

conn = connPool.getconn()
cur = conn.cursor()
cur.execute("select version()")
records = cur.fetchall()
print("success")
print(records)