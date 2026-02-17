import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="assessment_db",
        user="postgres",
        password="Tiger"
    )
