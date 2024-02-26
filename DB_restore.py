import pymssql
import mysql.connector
from Database import Database
import time
from datetime import datetime
import os

def execute_sql_statement(sql_statement, dbname=None):
    try:
        conn = pymssql.connect(server='server', user='sa', password='passwd', database='master', autocommit=True)
        cursor = conn.cursor()
        cursor.execute(sql_statement)
        conn.close()
        if dbname:
            print(f"SQL statements executed successfully for database: {dbname}")
        else:
            print("SQL statements executed successfully.")
    except pymssql.Error as e:
        error_description = str(e)
        print("Error executing SQL statement:", error_description)
        log_error(error_description, dbname)

#log errors
def log_error(description, dbname=None):
    try:
        db = Database()
        connection_status = db.check_connection()
        if connection_status:
            current_time = time.strftime('%Y-%m-%d %H:%M:%S')
            query = "INSERT INTO logs (type, description, time) VALUES (%s, %s, %s)"
            values = ('Error', f"Restore faild for database {dbname} {description}", current_time)
            db.execute_update(query, values)
            db.close()
            print("Error logged to database.")
        else:
            print("Error: Could not log to database. Connection not established.")
    except Exception as e:
        print("Error logging to database:", str(e))

#log_status
def log_status(status, dbname):
    try:
        db = Database()
        connection_status = db.check_connection()
        if connection_status:
            current_time = time.strftime('%Y-%m-%d %H:%M:%S')
            query = "INSERT INTO logs (type, description, time ) VALUES (%s, %s, %s)"
            values = ('Success', f"Restore {status} for database {dbname}", current_time)
            db.execute_update(query, values)
            db.close()
            print("Status logged to database.")
        else:
            print("Error: Could not log status to database. Connection not established.")
    except Exception as e:
        print("Error logging status to database:", str(e))

def get_current_date():
    current_date = datetime.now().date()
    formatted_date = current_date.strftime('%Y-%m-%d')
    return formatted_date

def get_backup_file(folder_path, file_prefix):
    for file in os.listdir(folder_path):
        if file.startswith(file_prefix) and file.endswith(".bak"):
            return os.path.join(folder_path, file)
    return None

#Database names 
databases = {
    "students": "students",
    "school": "school",
    "course": "course"
}

curent_date = get_current_date()
#backup folder path
folder_path = "D:\\backup_DB\\{curent_date}"

backup_files = {}

for dbname, file_prefix in databases.items():
    backup_file = get_backup_file(folder_path, file_prefix)
    if backup_file:
        print(backup_file)
        backup_files[dbname] = backup_file
    else:
        print(f"No backup file found for {dbname}")
        log_status(f"No backup file found for {dbname}")

# Execute restore 
for dbname, backup_file in backup_files.items():
    task1_sql = f"ALTER DATABASE [{dbname}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
    task2_sql = f"""
        RESTORE DATABASE [{dbname}] FROM DISK = N'{backup_file}' WITH FILE = 1, 
        MOVE N'{dbname}' TO N'C:\\Program Files\\Microsoft SQL Server\\MSSQL16.MSSQLSERVER\\MSSQL\\DATA\\{dbname}.mdf', 
        MOVE N'{dbname}_log' TO N'C:\\Program Files\\Microsoft SQL Server\\MSSQL16.MSSQLSERVER\\MSSQL\\DATA\\{dbname}_log.ldf', 
        NOUNLOAD, REPLACE, STATS = 5
        """
    task3_sql = f"ALTER DATABASE [{dbname}] SET MULTI_USER"
        
    execute_sql_statement(task1_sql)
    execute_sql_statement(task2_sql, dbname)
    execute_sql_statement(task3_sql)
    log_status("completed successfully", dbname)
        
