import mysql.connector

class Database:
    def __init__(self):
        self.DB_URL = "localhost"
        self.DB_USER = "user"
        self.DB_PASSWORD = "passwd"
        self.connection = mysql.connector.connect(host=self.DB_URL, user=self.DB_USER, password=self.DB_PASSWORD, database="Backup_logs")

    def close(self):
        if self.connection.is_connected():
            self.connection.close()

    def check_connection(self):
        return self.connection.is_connected()

    def execute_query(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result

    def execute_update(self, query, values):
        cursor = self.connection.cursor()
        cursor.execute(query, values)
        self.connection.commit()
        cursor.close()
    

