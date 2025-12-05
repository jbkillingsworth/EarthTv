import psycopg2
from dotenv import load_dotenv
import os

class Postgres:
    def __init__(self):
        load_dotenv()

        try:
            self.conn = psycopg2.connect(
                host=os.getenv("DB_HOST"),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PW"),
                port=os.getenv("DB_PORT")
            )

            self.cur = self.conn.cursor()

        except psycopg2.Error as e:
            print(f"Error connecting to PostgreSQL: {e}")

    def get_version(self):
        return self.cur.execute("SELECT version();")

if __name__ == '__main__':
    Postgres()