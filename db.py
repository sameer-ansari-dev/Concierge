import psycopg2

def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="concierge_db",
        user="postgres",         
        password="password" 
    )
    return conn

