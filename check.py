import psycopg2

try:
    connection = psycopg2.connect(
        user="postgres",
        password="password",
        host="127.0.0.1",
        port="5432",
        database="postgres2"
    )
    # The rest of your code to interact with the database...
    print("Successfully connected to the database!")

except Exception as error:
    print("Error connecting to database:", error)

finally:
    if 'connection' in locals() and connection:
        connection.close()