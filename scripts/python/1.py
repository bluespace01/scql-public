import mysql.connector

def create_connection():
    try:
        conn = mysql.connector.connect(
            user='root',
            password='123456',
            host='192.168.10.13',
            port=3306,
            database='alice'
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

connection = create_connection()  # Ensure this is called before using `connection`

