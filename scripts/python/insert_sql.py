from faker import Faker
import mysql.connector

# 数据库配置参数
# Alice
alice_db_config = {
    'user': 'root',
    'password': '123456',
    'host': '192.168.10.121',
    'port': 3306,
    'database': 'alice'  # 这里可以是想要连接的数据库名
}

# Bob
bob_db_config = {
    'user': 'root',
    'password': '123456',
    'host': '192.168.10.122',
    'port': 3306,
    'database': 'bob'  # 这里可以是想要连接的数据库名
}

# 设置生成数据的数量
num_records = 1000000  # 1M 数据

fake = Faker()
#--------------------------------------------------------------------------
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

#--------------------------------------------------------------------------
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

#--------------------------------------------------------------------------
def create_table_if_not_exists_alice(cursor):
    """如果表存在，则删除表"""
    cursor.execute("DROP TABLE IF EXISTS user_credit")

    """如果表不存在，则创建表"""
    cursor.execute("""
    CREATE TABLE `user_credit` (
        `ID` varchar(64) NOT NULL,
        `credit_rank` int NOT NULL,
        `income` int NOT NULL,
        `age` int NOT NULL,
        PRIMARY KEY (`ID`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

#--------------------------------------------------------------------------
def create_table_if_not_exists_bob(cursor):
    """如果表存在，则删除表"""
    cursor.execute("DROP TABLE IF EXISTS user_stats")

    """如果表不存在，则创建表"""
    cursor.execute("""
    CREATE TABLE `user_stats` (
        `ID` varchar(64) not NULL,
        `order_amount` float not null,
        `is_active` tinyint(1) not null,
        PRIMARY KEY (`ID`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

#--------------------------------------------------------------------------
def insert_data_alice(conn, cursor, num_records, batch_size=1000):
    """批量插入数据"""
    records = []
    for i in range(num_records):
        # 生成随机数据
        id = 'id' + str(i+1).zfill(9)
        credit_rank = fake.random_int(min=3, max=6)
        income = fake.random_int(min=10000, max=99999)
        age = fake.random_int(min=18, max=50)

        # 添加到记录列表
        records.append((id, credit_rank, income, age))

        # 每 batch_size 条数据执行一次插入
        if len(records) >= batch_size:
            sql = """
            INSERT INTO user_credit (id, credit_rank, income, age)
            VALUES (%s, %s, %s, %s)
            """
            cursor.executemany(sql, records)
            conn.commit()
            records = []  # 清空记录

    # 插入剩余的记录
    if records:
        sql = """
        INSERT INTO user_credit (id, credit_rank, income, age)
        VALUES (%s, %s, %s, %s)
        """
        cursor.executemany(sql, records)
        conn.commit()

#--------------------------------------------------------------------------
def insert_data_bob(conn, cursor, num_records, batch_size=1000):
    """批量插入数据"""
    records = []
    for i in range(num_records):
        # 生成随机数据
        id = 'id' + str(i+1).zfill(9)
        amount = fake.pyfloat(left_digits=5, right_digits=2, positive=True, min_value=100, max_value=100000)
        activate = 1 if fake.pybool() else 0

        # 添加到记录列表
        records.append((id, amount, activate))

        # 每 batch_size 条数据执行一次插入
        if len(records) >= batch_size:
            sql = """
            INSERT INTO user_stats (id, order_amount, is_active)
            VALUES (%s, %s, %s)
            """
            cursor.executemany(sql, records)
            conn.commit()
            records = []  # 清空记录

    # 插入剩余的记录
    if records:
        sql = """
        INSERT INTO user_stats (id, order_amount, is_active)
        VALUES (%s, %s, %s)
        """
        cursor.executemany(sql, records)
        conn.commit()


#--------------------------------------------------------------------------
def main():
    conn = None
    cursor = None

    try:
        # 连接到 bob MySQL 服务器
        conn = create_connection(alice_db_config)

        if conn and conn.is_connected():
            print('Successfully connected to the MySQL alice server')

            # 选择或创建数据库
            select_or_create_database(conn, alice_db_config['database'])

            # 使用数据库
            cursor = conn.cursor()
            cursor.execute(f"USE {alice_db_config['database']}")

            # 创建表（如果不存在）
            create_table_if_not_exists_alice(cursor)

            # 插入数据
            insert_data_alice(conn, cursor, num_records)

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


    try:
        # 连接到 bob MySQL 服务器
        conn = create_connection(bob_db_config)

        if conn and conn.is_connected():
            print('Successfully connected to the MySQL bob server')

            # 选择或创建数据库
            select_or_create_database(conn, bob_db_config['database'])

            # 使用数据库
            cursor = conn.cursor()
            cursor.execute(f"USE {bob_db_config['database']}")

            # 创建表（如果不存在）
            create_table_if_not_exists_bob(cursor)

            # 插入数据
            insert_data_bob(conn, cursor, num_records)

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

#--------------------------------------------------------------------------
if __name__ == "__main__":
    main()
