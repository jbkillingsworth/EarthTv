import psycopg2
from dotenv import load_dotenv
import os

class Postgres:
    def __init__(self):
        load_dotenv()

        try:
            self.conn = psycopg2.connect(
                host='database',#os.getenv("DB_HOST"),
                database='postgres',#os.getenv("DB_NAME"),
                user='postgres',#os.getenv("DB_USER"),
                password='postgres',#os.getenv("DB_PW"),
                port='5432'#os.getenv("DB_PORT")
            )

            self.cur = self.conn.cursor()

        except psycopg2.Error as e:
            print(f"Error connecting to PostgreSQL: {e}")

    def get_version(self):
        return self.cur.execute("SELECT version();")

if __name__ == '__main__':
    Postgres()