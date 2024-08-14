from faker import Faker
import mysql.connector
from mysql.connector import errorcode

fake = Faker()

# 数据库配置参数
# Alice
# db_config = {
#     'user': 'root',
#     'password': '123456',
#     'host': '192.168.10.121',
#     'port': 3306,
#     'database': 'test'  # 这里可以是想要连接的数据库名
# }


# Bob
db_config = {
    'user': 'root',
    'password': '123456',
    'host': '192.168.10.122',
    'port': 3306,
    'database': 'test'  # 这里可以是想要连接的数据库名
}


# 设置生成数据的数量
num_records = 10000  # 1M 数据

def create_connection(config):
    """创建 MySQL 连接"""
    try:
        # 尝试连接到 MySQL 服务器，但不指定数据库
        conn = mysql.connector.connect(
            user=config['user'],
            password=config['password'],
            host=config['host'],
            port=config['port']
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def select_or_create_database(conn, database_name):
    """选择数据库，如果不存在则创建数据库"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        cursor.execute(f"USE {database_name}")
    except mysql.connector.Error as err:
        print(f"Error while selecting or creating database: {err}")
    finally:
        cursor.close()

def create_table_if_not_exists(cursor):
    """如果表不存在，则创建表"""
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(100),
        address VARCHAR(255),
        date_of_birth DATE
    )
    """)

def insert_data(conn, cursor, num_records):
    """插入数据"""
    for _ in range(num_records):
        name = fake.name().replace("'", "''")
        email = fake.email().replace("'", "''")
        address = fake.address().replace("'", "''")
        dob = fake.date_of_birth(tzinfo=None, minimum_age=18, maximum_age=90).isoformat()

        # 构造插入语句
        sql = """
        INSERT INTO users (name, email, address, date_of_birth)
        VALUES (%s, %s, %s, %s)
        """
        values = (name, email, address, dob)

        # 执行插入语句
        cursor.execute(sql, values)

        # 每 1000 条数据提交一次
        if cursor.rowcount % 1000 == 0:
            conn.commit()
    
    # 提交剩余数据
    conn.commit()

def main():
    conn = None
    cursor = None

    try:
        # 连接到 MySQL 服务器
        conn = create_connection(db_config)

        if conn and conn.is_connected():
            print('Successfully connected to the MySQL server')

            # 选择或创建数据库
            select_or_create_database(conn, db_config['database'])

            # 使用数据库
            cursor = conn.cursor()
            cursor.execute(f"USE {db_config['database']}")

            # 创建表（如果不存在）
            create_table_if_not_exists(cursor)



            # 插入数据
            insert_data(conn, cursor, num_records)

        else:
            print("Failed to connect to the MySQL server.")

    except mysql.connector.Error as err:
        print(f"An unexpected error occurred: {err}")
    finally:
        # 确保游标和连接都被关闭
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

    print('Data insertion successfully.')

if __name__ == "__main__":
    main()
