import pyodbc

def check_sql_server_connection(server, database, username, password):
    try:
        connection_string = f"DRIVER=SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}"
        connection = pyodbc.connect(connection_string)
        connection.close()
        return True
    except pyodbc.Error:
        return False

if __name__ == "__main__":
    server_name = "172.16.0.28"
    database_name = "DB_PRMITR_ERP_20230701"
    user_name = "Swarnim_Intern"
    password = "Swarnim14#"

    if check_sql_server_connection(server_name, database_name, user_name, password):
        print("Connection to SQL Server is available.")
    else:
        print("Connection to SQL Server is not available.")
