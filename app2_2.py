import pyodbc
import pandas as pd
import openpyxl


# Connect to the source database
source_server = "172.16.0.28"
source_db = "DB_PRMITR_ERP_20230701"
source_user = "Swarnim_Intern"
source_password = "Swarnim14#"
source_connection_string = f"DRIVER=SQL Server;SERVER={source_server};DATABASE={source_db};UID={source_user};PWD={source_password}"


source_connection = pyodbc.connect(source_connection_string)
source_cursor = source_connection.cursor()
source_cursor.execute("SELECT name FROM sys.procedures WHERE type_desc = 'SQL_STORED_PROCEDURE'")
source_sp_list = [row.name for row in source_cursor.fetchall()]

# Connect to the target database
target_server = "172.16.0.28"
target_db = "DB_PRMITR_ERP_20230615"
target_user = "Swarnim_Intern"
target_password = "Swarnim14#"
target_connection_string = f"DRIVER=SQL Server;SERVER={target_server};DATABASE={target_db};UID={target_user};PWD={target_password}"

target_connection = pyodbc.connect(target_connection_string)
target_cursor = target_connection.cursor()
target_cursor.execute("SELECT name FROM sys.procedures WHERE type_desc = 'SQL_STORED_PROCEDURE'")
target_sp_list = [row.name for row in target_cursor.fetchall()]


# for procedure_name in source_sp_list:
#     source_cursor.execute(f"EXEC sp_helptext '{procedure_name}'")
#     rows = source_cursor.fetchall()
#     definition = "".join([row[0] for row in rows])
#     print(definition)
#     print("\n\n")

for sp_name in source_sp_list:
    source_cursor.execute(f"EXEC sp_helptext '{sp_name}'")
    source_sp_rows = source_cursor.fetchall()
    source_definition = "".join([row[0] for row in source_sp_rows])

    if sp_name in target_sp_list:
        target_cursor.execute(f"EXEC sp_helptext '{sp_name}'")
        target_sp_rows = target_cursor.fetchall()
        target_definition = "".join([row[0] for row in target_sp_rows])

        if source_definition == target_definition:
            print(f"{sp_name} is EQUAL")
        else:
            print(f"{sp_name} is UNEQUAL")
    else:
        print(f"{sp_name} is ABSENT")




# Close database connections
source_cursor.close()
source_connection.close()
target_cursor.close()
target_connection.close()