import os
import codecs
import chardet  # You may need to install this library
import signal
import datetime
from os.path import exists
import re
import datetime
from openpyxl import load_workbook
import openpyxl
from openpyxl.styles import PatternFill
import pandas as pd
import pyodbc
import nltk
import os
import difflib
import sqlparse
import re
import sys
import pandas as pd
from tqdm import tqdm

# ANSI escape codes for text colors
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RESET = "\033[0m"


def fetch_schema(server, database, username, password):
    schema_info = {}

    try:
        # Establish connection:
        connection_string = f"DRIVER=SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}"
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        # Fetch Schema
        schema_query = """
        SELECT
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.TABLES AS t
        JOIN INFORMATION_SCHEMA.COLUMNS AS c ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
        """

        print("\nExecuting the schema query. Please wait...\n")
        cursor.execute(schema_query)
        print("Schema query execution" + GREEN + " completed" + RESET + ".\n")
        rows = cursor.fetchall()

        for row in rows:
            table_schema = row.TABLE_SCHEMA
            table_name = row.TABLE_NAME
            column_name = row.COLUMN_NAME
            data_type = row.DATA_TYPE
            max_length = row.CHARACTER_MAXIMUM_LENGTH
            numeric_precision = row.NUMERIC_PRECISION
            numeric_scale = row.NUMERIC_SCALE

            schema_info.setdefault(table_schema, {}).setdefault(table_name, []).append(
                {
                    "column_name": column_name,
                    "data_type": data_type,
                    "max_length": max_length,
                    "numeric_precision": numeric_precision,
                    "numeric_scale": numeric_scale,
                }
            )

        connection.close()

        return schema_info
    except pyodbc.Error as e:
        raise Exception(RED + "Error " + RESET + f"while fetching schema: {str(e)}")
    except Exception as ex:
        raise Exception(
            "An" + RED + " unexpected error " + RESET + f"occurred: {str(ex)}"
        )


def generate_excel_report(workbook, source_schema, target_schema, target_db_name):
    comparison_results = []
    comparison_results = perform_schema_comparison(source_schema, target_schema)
    sheet = workbook.create_sheet(target_db_name)

    # Write Headers
    sheet["A1"] = "Schema"
    sheet["B1"] = "Table Name"
    sheet["C1"] = "Column Name"
    sheet["D1"] = "Type of Error"
    sheet["E1"] = "Max Length"
    sheet["F1"] = "Numeric Precision"
    sheet["G1"] = "Numeric Scale"

    # Populate the sheet
    row = 2
    for result in comparison_results:
        sheet[f"A{row}"] = result["schema"]
        sheet[f"B{row}"] = result["table_name"]
        sheet[f"C{row}"] = result["column_name"]
        sheet[f"D{row}"] = result["data_type"]
        sheet[f"E{row}"] = result["max_length"]
        sheet[f"F{row}"] = result["numeric_precision"]
        sheet[f"G{row}"] = result["numeric_scale"]

        if result["data_type"] == "Missing Column":
            sheet[f"D{row}"].fill = PatternFill(
                start_color="F9B9B7", end_color="F9B9B7", fill_type="solid"
            )
        elif result["data_type"] == "Different Specification":
            sheet[f"D{row}"].fill = PatternFill(
                start_color="D1D5DE", end_color="D1D5DE", fill_type="solid"
            )
        row = row + 1


def perform_schema_comparison(source_schema, target_schema):
    comparison_results = []
    for schema in source_schema:
        for table_name in source_schema[schema]:
            source_columns = source_schema[schema][table_name]
            target_columns = target_schema[schema].get(table_name, [])

            for col_info_source in source_columns:
                col_name_source = col_info_source["column_name"]
                col_info_target = next(
                    (
                        col
                        for col in target_columns
                        if col["column_name"] == col_name_source
                    ),
                    None,
                )

                if col_info_target is None:
                    comparison_result = {
                        "schema": schema,
                        "table_name": table_name,
                        "column_name": col_name_source,
                        "data_type": "Missing Column",
                        "max_length": str(col_info_source),
                        "numeric_precision": "",
                        "numeric_scale": "",
                    }
                    comparison_results.append(comparison_result)

                elif col_info_source != col_info_target:
                    comparison_result = {
                        "schema": schema,
                        "table_name": table_name,
                        "column_name": col_name_source,
                        "data_type": "Different Specification",
                        "max_length": str(col_info_source),
                        "numeric_precision": str(col_info_target),
                        "numeric_scale": "",
                    }
                    comparison_results.append(comparison_result)

    return comparison_results


def app1():
    try:
        print("Enter details of" + GREEN + " SOURCE " + RESET + "database: ")
        source_info = {
            "server": 1234,
            "database": "dbdb",
            "username": "uid",
            "password": "pwd",
        }

        source_info["server"] = input(
            "Enter" + GREEN + " Server " + RESET + "Address:\t"
        )
        source_info["database"] = input(
            "Enter" + GREEN + " Database " + RESET + "Name:\t"
        )
        source_info["username"] = input("Enter Server" + GREEN + " Username:\t" + RESET)
        source_info["password"] = input("Enter Server" + GREEN + " Password:\t" + RESET)

        source_server = source_info["server"]
        source_database = source_info["database"]
        source_username = source_info["username"]
        source_password = source_info["password"]

        if (
            not source_server
            or not source_database
            or not source_username
            or not source_password
        ):
            raise ValueError(
                "Source database details are"
                + RED
                + " incomplete"
                + RESET
                + ". Please provide all required information."
            )

        print("Fetching Source Information... \n")

        try:
            source_schema = fetch_schema(
                source_server, source_database, source_username, source_password
            )  # has information regarding source schema
        except Exception as e:
            raise ValueError(
                f"Couldn't fetch source schema. Perhaps the"
                + RED
                + " login details "
                + RESET
                + "or"
                + RED
                + " database details "
                + RESET
                + "you entered are incorrect?\n"
            )

        # Create a single workbook outside the loop
        workbook = openpyxl.Workbook()

        try:
            number_of_target_db = input(
                "How many databases do you want to compare against source? \t"
            )
            number_of_target_db = int(number_of_target_db)
        except Exception as e:
            print(
                RED
                + "Error: "
                + RESET
                + "No input or incorrect form of input was provided."
            )
            return

        nested_target_dbs = {}
        number_of_target_db_copy = number_of_target_db

        if number_of_target_db <= 0:
            print("No databases to compare." + RED + " Exiting." + RESET)
            return

        for target_db_number in range(1, number_of_target_db + 1):
            # Initialize the nested dictionary for the current target DB number
            nested_target_dbs[target_db_number] = {}
            nested_target_dbs[target_db_number]["server"] = input(
                f"\nEnter"
                + GREEN
                + " Server Address "
                + RESET
                + f"for Target DB number {target_db_number}: "
            )
            nested_target_dbs[target_db_number]["database"] = input(
                f"Enter"
                + GREEN
                + " Database Name "
                + RESET
                + f"for Target DB number {target_db_number}: "
            )
            nested_target_dbs[target_db_number]["username"] = input(
                f"Enter"
                + GREEN
                + " User Name "
                + RESET
                + f"for Target DB number {target_db_number}: "
            )
            nested_target_dbs[target_db_number]["password"] = input(
                f"Enter"
                + GREEN
                + " Password "
                + RESET
                + f"for Target DB number {target_db_number}: "
            )

            target_server = nested_target_dbs[target_db_number]["server"]
            target_database = nested_target_dbs[target_db_number]["database"]
            target_username = nested_target_dbs[target_db_number]["username"]
            target_password = nested_target_dbs[target_db_number]["password"]

            if (
                not target_server
                or not target_database
                or not target_username
                or not target_password
            ):
                print(
                    f"Target database number {target_db_number}'s details are"
                    + RED
                    + " incomplete"
                    + RESET
                    + ". Skipping comparison for this target."
                )
            else:
                try:
                    target_schema = fetch_schema(
                        target_server, target_database, target_username, target_password
                    )
                    generate_excel_report(
                        workbook,
                        source_schema,
                        target_schema,
                        nested_target_dbs[target_db_number]["database"],
                    )

                except Exception as e:
                    print(
                        f"\nError fetching schema for target database number {target_db_number}: {str(e)}"
                    )

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
        excel_file_name = f"Schema_Comparison_Report_{timestamp}.xlsx"

        workbook.remove(workbook.active)
        workbook.save(excel_file_name)
        print(
            f"\nExcel file"
            + GREEN
            + " successfully "
            + RESET
            + f"created at {os.path.abspath(excel_file_name)}\n\n"
        )

        summary = {}

        excel_file = openpyxl.load_workbook(excel_file_name)

        for sheet_name in excel_file.sheetnames:
            sheet = excel_file[sheet_name]

            missing_columns = 0
            different_specifications = 0
            total_differences = 0

            for row in sheet.iter_rows(min_row=2, values_only=True):
                data_type = row[3]

                if data_type == "Missing Column":
                    missing_columns = missing_columns + 1
                elif data_type == "Different Specification":
                    different_specifications = different_specifications + 1

            total_differences = missing_columns + different_specifications

            summary[sheet_name] = {
                "Missing Columns": missing_columns,
                "Different Specifications": different_specifications,
                "Total Differences": total_differences,
            }

        excel_file.close()

        print(YELLOW + "Summary: \n" + RESET)

        for target_db, target_summary in summary.items():
            print(YELLOW + "\nTarget Database: " + RESET + f"{target_db}")
            print(f"Missing Columns: {target_summary['Missing Columns']}")
            print(
                f"Different Specifications: {target_summary['Different Specifications']}"
            )
            print(f"Total Differences: {target_summary['Total Differences']}\n")

        print(
            f"\nNumber of databases compared against source: {number_of_target_db_copy}"
        )
        os.system(f'explorer /select,"{os.path.abspath(excel_file_name)}"')
        input("\n\nPress Enter to exit...")
    except ValueError as ve:
        print(f"Error: {ve}")
    except Exception as e:
        print(f"Error: {str(e)}")


# END OF APP 1 #########################################################################


def fetch_stored_procedures(server, database, username, password):
    stored_procedures = []

    try:
        # Establish a connection to the database
        connection_string = f"DRIVER=SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}"
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        # Fetch stored procedures
        query = """
        SELECT name
        FROM sys.procedures
        WHERE type_desc = 'SQL_STORED_PROCEDURE'
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            stored_procedures.append(row[0])

        connection.close()

    except pyodbc.Error as e:
        print(f"Error: {str(e)}")

    return stored_procedures


def strip_comments(sql_file_contents):
    try:
        stripped_sql_file = re.sub(r"--.*", "", sql_file_contents)
        stripped_sql_file = re.sub(r"/\*.*?\*/", "", stripped_sql_file, flags=re.DOTALL)
        stripped_sql_file = "\n".join(
            " ".join(part.strip() for part in line.split() if part.strip())
            for line in stripped_sql_file.splitlines()
            if line.strip()
        )
    except Exception as e:
        print(f"Error while stripping comments: {str(e)}")
        stripped_sql_file = ""

    return stripped_sql_file


def difference(source_sql_path, test_sql_path) -> bool:
    try:
        try:
            with open(source_sql_path, "r", encoding="utf-8") as file:
                source_contents = file.read().upper()
        except UnicodeError:
            with open(source_sql_path, "r", encoding="utf-16") as file:
                source_contents = file.read().upper()

        try:
            with open(test_sql_path, "r", encoding="utf-8") as file:
                test_contents = file.read().upper()
        except UnicodeError:
            with open(test_sql_path, "r", encoding="utf-16") as file:
                test_contents = file.read().upper()

        stripped_sql_file_source = strip_comments(source_contents)
        stripped_sql_file_test = strip_comments(test_contents)

        if stripped_sql_file_source == stripped_sql_file_test:
            return True
        else:
            return False

    except Exception as e:
        print(f"Error while comparing SQL Files: {str(e)}")

def download_stored_procedure(server, database, username, password, sp_name, output_file_path):
    try:
        # Establish a connection to the SQL Server
        connection_string = f"DRIVER=SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}"
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        # Define a query to fetch the stored procedure text
        query = f"EXEC sp_helptext '{sp_name}'"

        # Execute the query to fetch the stored procedure text
        cursor.execute(query)

        # Fetch all lines of the stored procedure
        stored_procedure_text = "\n".join([row[0] for row in cursor.fetchall()])

        # Write the stored procedure text to the output file
        with open(output_file_path, 'w', encoding='utf-8') as output_file:
            output_file.write(stored_procedure_text)

        print(f"Stored procedure '{sp_name}' downloaded to '{output_file_path}'.")

        # Close the connection
        connection.close()

    except pyodbc.Error as e:
        print(f"Error: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")


def app2_2__():

    print("Enter Source Database Details:")
    source_server = input("Enter server address:\t")
    source_database = input("Enter database name:\t")
    source_username = input("Enter username:\t")
    source_password = input("Enter password:\t")

    source_stored_procedures = fetch_stored_procedures(source_server, source_database, source_username, source_password)

    print(source_stored_procedures)

    # print("\n\n")

    for i in range(len(source_stored_procedures)):
        output_file_path = f"C:\\Users\\swarn\\github\\Sequel\\testsql\\{source_stored_procedures[i]}"
        sqlfile = download_stored_procedure(source_server, source_database, source_username, source_password, source_stored_procedures[i], output_file_path)
        print("\n\n")
        print(sqlfile)


    num_of_target_dbs = int(input("How many target databases do you want to compare against source?\t"))

    target_db_dic = {}
    print("Enter details for target databases:\n")
    for i in num_of_target_dbs:
        target_db_dic[i]['Server'] = input(f"Enter Server Address for target database number {i}:\t")
        target_db_dic[i]['Database'] = input(f"Enter Database Name for target database number {i}:\t")
        target_db_dic[i]['Username'] = input(f"Enter Username for target database number {i}:\t")
        target_db_dic[i]['Password'] = input(f"Enter Password for target database number {i}:\t")

    # print(target_db_dic)


    # try:
    #     sp_data = []
    #     summary = {}

    #     for target_db_dir in target_db_dirs:
    #         summary[target_db_dir] = {
    #             "Absent Entries": 0,
    #             "Present & Unequal Entries": 0,
    #             "Present & Equal Entries": 0,
    #         }

    #     for sp_name in stored_procedures:
    #         sp_info = {"SP Name": sp_name}
    #         source_sql_path = (
    #             f"online_db_{sp_name}.sql"  # You can customize the filename as needed
    #         )

    #         for target_db_dir in target_db_dirs:
    #             target_sql_path = os.path.join(target_db_dir, f"{sp_name}.sql")
    #             if os.path.exists(target_sql_path):
    #                 if difference(source_sql_path, target_sql_path):
    #                     sp_info[os.path.basename(target_db_dir)] = "PRESENT & EQUAL"
    #                     summary[target_db_dir]["Present & Equal Entries"] += 1
    #                 else:
    #                     sp_info[os.path.basename(target_db_dir)] = "PRESENT & UNEQUAL"
    #                     summary[target_db_dir]["Present & Unequal Entries"] += 1
    #             else:
    #                 sp_info[os.path.basename(target_db_dir)] = "ABSENT"
    #                 summary[target_db_dir]["Absent Entries"] += 1

    #         sp_data.append(sp_info)

    #     df = pd.DataFrame(sp_data)

    #     timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    #     output_excel_path = f"SP_Comparison_Report_{timestamp}.xlsx"
    #     df.to_excel(output_excel_path, index=False)
    #     # Load the existing workbook and sheet
    #     wb = load_workbook(output_excel_path)
    #     ws = wb.active

    #     # Apply cell coloring based on the cell values
    #     for row in ws.iter_rows(
    #         min_row=2, max_row=ws.max_row, min_col=2, max_col=ws.max_column
    #     ):
    #         for cell in row:
    #             if cell.value == "PRESENT & UNEQUAL":
    #                 cell.fill = PatternFill(
    #                     start_color="FCD5B4", end_color="FCD5B4", fill_type="solid"
    #                 )
    #             elif cell.value == "ABSENT":
    #                 cell.fill = PatternFill(
    #                     start_color="E6B8B7", end_color="E6B8B7", fill_type="solid"
    #                 )

    #     # Save the modified workbook
    #     wb.save(output_excel_path)

    #     # Print the summary for each target database
    #     print(YELLOW + "\n\nSummary:\n\n" + RESET)
    #     for target_db_dir, summary_data in summary.items():
    #         print(
    #             YELLOW
    #             + "\nTarget Database: "
    #             + RESET
    #             + GREEN
    #             + f"{os.path.basename(target_db_dir)}"
    #             + RESET
    #         )
    #         print(f"Absent Entries: {summary_data['Absent Entries']}")
    #         print(
    #             f"Present & Unequal Entries: {summary_data['Present & Unequal Entries']}"
    #         )
    #         print(f"Present & Equal Entries: {summary_data['Present & Equal Entries']}")
    #         print(
    #             f"Total Entries Scanned Against Source: {summary_data['Absent Entries']+summary_data['Present & Unequal Entries']+summary_data['Present & Equal Entries']}"
    #         )
    #         print("\n")

    #     excel_absolute_path = os.path.abspath(output_excel_path)
    #     print(
    #         GREEN
    #         + "\nSuccess: "
    #         + RESET
    #         + f"Excel Report has been generated at {excel_absolute_path}\n"
    #     )
    #     os.system(f'explorer /select, "{os.path.abspath(output_excel_path)}"')
    #     input("Press Enter to exit...")

    # except Exception as e:
    #     print(f"An unexpected error occurred: {str(e)}")

## APP 2_2()

def app2_2():
    pass





def app2_1():
    try:
        print("Enter details of your" + GREEN + " Source Database " + RESET + ":\n")
        source_db_dir = input(
            "Enter the Source Database" + GREEN + " directory location" + RESET + ":\t"
        )
        print("\n")

        if not source_db_dir.strip():
            print(
                RED
                + "Error: "
                + RESET
                + "No source database provided. Exiting program...\n\n"
            )
            sys.exit(1)

        if not os.path.isdir(source_db_dir):
            print(
                RED
                + "Error: "
                + RESET
                + f"Directory '{source_db_dir}' is invalid. Exiting program..."
            )
            sys.exit(1)

        num_target_dbs = input(
            "Enter"
            + GREEN
            + " number of Target Databases "
            + RESET
            + "you want to compare:\t"
        )

        if not num_target_dbs:
            print(
                RED
                + "\n\nError: "
                + RESET
                + "No input provided. Exiting the program...\n\n"
            )
            sys.exit(1)

        try:
            num_target_dbs = int(num_target_dbs)
        except ValueError:
            print(
                RED
                + "\n\nError: "
                + RESET
                + "Invalid input. Please enter a valid positive integer.\n\n"
            )
            sys.exit(1)

        target_db_dirs = []

        for i in range(num_target_dbs):
            while True:
                target_db_dir = input(
                    "\nEnter "
                    + GREEN
                    + "target database directory location "
                    + RESET
                    + f"for target database number {i+1}:\t"
                )

                if not target_db_dir.strip():
                    print(
                        RED
                        + "Error: "
                        + RESET
                        + "No input provided. Please enter a directory location.\n"
                    )
                elif not os.path.exists(target_db_dir):
                    print(
                        RED
                        + "Error: "
                        + RESET
                        + f"The directory '{target_db_dir}' does not exist or is invalid. Please enter a valid directory location.\n"
                    )
                else:
                    target_db_dirs.append(target_db_dir)
                    break  # Valid input, exit the loop

        sp_data = []

        # Get the list of sql file in source database
        source_sql_file_list = os.listdir(source_db_dir)

        summary = {}

        for target_db_dir in target_db_dirs:
            summary[target_db_dir] = {
                "Absent Entries": 0,
                "Present & Unequal Entries": 0,
                "Present & Equal Entries": 0,
            }

        for sql_file in source_sql_file_list:
            sp_name = sql_file[:-4]  # Remove the .sql extension
            sp_info = {"SP Name": sp_name}
            source_sql_path = os.path.join(source_db_dir, sql_file)

            for target_db_dir in target_db_dirs:
                target_sql_path = os.path.join(target_db_dir, sql_file)
                if os.path.exists(target_sql_path):
                    if difference(source_sql_path, target_sql_path):
                        sp_info[os.path.basename(target_db_dir)] = "PRESENT & EQUAL"
                        summary[target_db_dir]["Present & Equal Entries"] += 1
                    else:
                        sp_info[os.path.basename(target_db_dir)] = "PRESENT & UNEQUAL"
                        summary[target_db_dir]["Present & Unequal Entries"] += 1
                else:
                    sp_info[os.path.basename(target_db_dir)] = "ABSENT"
                    summary[target_db_dir]["Absent Entries"] += 1

            sp_data.append(sp_info)

        df = pd.DataFrame(sp_data)

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
        output_excel_path = f"SP_Comparison_Report_{timestamp}.xlsx"
        df.to_excel(output_excel_path, index=False)
        # Load the existing workbook and sheet
        wb = load_workbook(output_excel_path)
        ws = wb.active

        # Apply cell coloring based on the cell values
        for row in ws.iter_rows(
            min_row=2, max_row=ws.max_row, min_col=2, max_col=ws.max_column
        ):
            for cell in row:
                if cell.value == "PRESENT & UNEQUAL":
                    cell.fill = PatternFill(
                        start_color="FCD5B4", end_color="FCD5B4", fill_type="solid"
                    )
                elif cell.value == "ABSENT":
                    cell.fill = PatternFill(
                        start_color="E6B8B7", end_color="E6B8B7", fill_type="solid"
                    )

        # Save the modified workbook
        wb.save(output_excel_path)

        # Print the summary for each target database
        print(YELLOW + "\n\nSummary:\n\n" + RESET)
        for target_db_dir, summary_data in summary.items():
            print(
                YELLOW
                + "\nTarget Database: "
                + RESET
                + GREEN
                + f"{os.path.basename(target_db_dir)}"
                + RESET
            )
            print(f"Absent Entries: {summary_data['Absent Entries']}")
            print(
                f"Present & Unequal Entries: {summary_data['Present & Unequal Entries']}"
            )
            print(f"Present & Equal Entries: {summary_data['Present & Equal Entries']}")
            print(
                f"Total Entries Scanned Against Source: {summary_data['Absent Entries']+summary_data['Present & Unequal Entries']+summary_data['Present & Equal Entries']}"
            )
            print("\n")

        excel_absolute_path = os.path.abspath(output_excel_path)
        print(
            GREEN
            + "\nSuccess: "
            + RESET
            + f"Excel Report has been generated at {excel_absolute_path}\n"
        )
        os.system(f'explorer /select, "{os.path.abspath(output_excel_path)}"')
        input("Press Enter to exit...")

    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")


def app2():
    online = input(
        "Do you want to carry out the SP comparison for online databases or offline stored database directories?\nEnter 1 for offline\nEnter 2 for online\nYour choice:\t\t"
    ).strip()
    online = int(online)

    match online:
        case 1:
            app2_1()
        case 2:
            app2_2()
        case _:
            print("Invalid choice. Exiting...")
            sys.exit(1)


# END OF APP 2 #########################################################################


def strip_sql_comments(sql_file_contents) -> str:
    # Remove single-line comments (--)
    stripped_sql_file = re.sub(r"--.*", "", sql_file_contents)
    # Remove multi-line comments (/* */)
    stripped_sql_file_1 = re.sub(r"/\*.*?\*/", "", stripped_sql_file, flags=re.DOTALL)
    # Normalize whitespace and line endings
    stripped_sql_file_2 = " ".join(
        line.strip() for line in stripped_sql_file_1.splitlines()
    )
    return stripped_sql_file_2


def normalize_sql(file_contents):
    tokens = nltk.word_tokenize(file_contents)
    file_contents_stringed = "".join(tokens)
    return file_contents_stringed


def generate_html_diff(file1_contents, file2_contents, folder1_path, folder2_path):
    folder1_name = os.path.basename(folder1_path)
    folder2_name = os.path.basename(folder2_path)

    file1_formatted = normalize_sql(file1_contents)
    file2_formatted = normalize_sql(file2_contents)
    # Remove comments and extra whitespace
    file1_formatted = strip_sql_comments(file1_contents).upper()
    file2_formatted = strip_sql_comments(file2_contents).upper()

    file1_formatted = sqlparse.format(
        file1_formatted, reindent=True, keyword_case="upper", strip_comments=True
    )
    file2_formatted = sqlparse.format(
        file2_formatted, reindent=True, keyword_case="upper", strip_comments=True
    )

    html_diff = difflib.HtmlDiff(tabsize=4, wrapcolumn=72).make_file(
        file1_formatted.splitlines(),
        file2_formatted.splitlines(),
        context=True,
        numlines=5,
        fromdesc=folder1_name,
        todesc=folder2_name,
    )
    return html_diff


def app3():
    comparison_results = []

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    excel_output_file = f"SP_Comparison_Repost__{timestamp}.xlsx"

    output_dir = f"Diff_Files_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)

    # Source Database
    folder1_path = input(
        "Enter location of the" + GREEN + " Source " + RESET + "database directory: \t"
    )
    if not folder1_path.strip():
        print(
            RED
            + "\nError: "
            + RESET
            + "No source database provided. Exiting program...\n"
        )
        sys.exit(1)

    if not os.path.isdir(folder1_path):
        print(
            RED
            + "\nError: "
            + RESET
            + f"The directory '{folder1_path}' is invalid. Exiting program...\n"
        )
        sys.exit(1)

    # Database to compare with source
    folder2_path = input(
        "Enter location of the" + GREEN + " Target " + RESET + "database directory: \t"
    )
    if not folder2_path.strip():
        print(
            RED
            + "\nError: "
            + RESET
            + "No source database provided. Exiting program...\n"
        )
        sys.exit(1)

    if not os.path.isdir(folder2_path):
        print(
            RED
            + "Error: "
            + RESET
            + f"The directory '{folder2_path}' is invalid. Exiting program...\n"
        )
        sys.exit(1)

    # Define cell fill colors
    different_color = PatternFill(
        start_color="8ab4ff", end_color="8ab4ff", fill_type="solid"
    )
    missing_color = PatternFill(
        start_color="ffd6ca", end_color="ffd6ca", fill_type="solid"
    )

    nltk.download("words", quiet=True)
    nltk.download("punkt", quiet=True)  # No output should be thrown on terminal when downloading
    nltk.download("words", quiet=True)
    for sql_file in os.listdir(folder1_path):  # Path of source will be passed here
        if sql_file.endswith(".sql"):
            file1_path = os.path.join(folder1_path, sql_file)
            file2_path = os.path.join(folder2_path, sql_file)

            sp_name = sql_file

            present_in_folder1 = os.path.exists(file1_path)
            present_in_folder2 = os.path.exists(file2_path)
            content_comparison = ""
            diff_file = ""

            if present_in_folder1 and present_in_folder2:
                try:
                    with open(file1_path, "r", encoding="utf-8") as file:
                        file1_contents = file.read().upper()
                except UnicodeError:
                    with open(file1_path, "r", encoding="utf-16") as file:
                        file1_contents = file.read().upper()
                normalized_sql_1 = normalize_sql(file1_contents)
                file1_nocomments = strip_sql_comments(normalized_sql_1)

                try:
                    with open(file2_path, "r", encoding="utf-8") as file:
                        file2_contents = file.read().upper()
                except UnicodeError:
                    with open(file2_path, "r", encoding="utf-16") as file:
                        file2_contents = file.read().upper()

                normalized_sql_2 = normalize_sql(file2_contents)
                file2_nocomments = strip_sql_comments(normalized_sql_2)

                # TODO implement difference
                if difference(file1_path, file2_path):
                    content_comparison = "Equal"
                    # print(f"Files {sql_file} are equal")
                else:
                    content_comparison = "Different"
                    # print(f"Files {sql_file} are unequal")
                    diff_html = generate_html_diff(
                        file1_contents, file2_contents, folder1_path, folder2_path
                    )
                    diff_filename = os.path.join(output_dir, f"diff_{sql_file}.html")

                    with open(diff_filename, "w") as diff_file:
                        diff_file.write(diff_html)
                    diff_file = diff_filename
            else:
                content_comparison = "Missing in one of the folders"
                if not present_in_folder1:
                    diff_file = f"Missing in {folder1_path}"
                else:
                    diff_file = f"Missing in {folder2_path}"

            comparison_results.append(
                [
                    sp_name,
                    present_in_folder1,
                    present_in_folder2,
                    content_comparison,
                    diff_file,
                ]
            )
    # Create a new Excel workbook and add a worksheet
    wb = openpyxl.Workbook()
    ws = wb.active

    # Define column headers
    headers = [
        "SP Name",
        f"Present in {os.path.dirname(folder1_path)}",
        f"Present in {os.path.dirname(folder2_path)}",
        "Content Comparison",
        "Diff File",
    ]

    # Write column headers to the worksheet
    ws.append(headers)

    # Write comparison results to the worksheet
    for row in comparison_results:
        ws.append(row)

    # Apply cell coloring based on conditions
    for row_idx, row in enumerate(
        ws.iter_rows(min_row=2, max_row=len(comparison_results) + 1), start=2
    ):
        if row[3].value == "Different":
            row[3].fill = different_color
        if row[2].value == False:
            row[2].fill = missing_color

    num_files_absent = sum(not row[2] for row in comparison_results)
    num_files_different = sum(row[3] == "Different" for row in comparison_results)
    total_files_scanned = len(comparison_results)

    # Save the workbook
    wb.save(excel_output_file)
    print(YELLOW + "\n\nSummary: " + RESET)
    print(
        "Number of files"
        + RED
        + " Absent "
        + RESET
        + f"in Target Database: {num_files_absent}"
    )
    print(
        "Number of files with"
        + RED
        + " Unequal Content "
        + RESET
        + f"in Target Database: {num_files_different}"
    )
    print(f"Total files scanned: {total_files_scanned}\n\n")
    print(f"Diff Files are stored in {os.path.abspath(output_dir)}")
    print(
        f"Excel file generated and stored in {os.path.dirname(os.path.abspath(excel_output_file))}\n\n"
    )
    # Open the folder insted of the excel file
    os.system(f'explorer /select, "{os.path.abspath(excel_output_file)}"')

    # END OF APP 3 #########################################################################


def main():
    try:
        print("\n\nPlease enter what program you wish to execute: \n\n")
        print(
            YELLOW
            + "1. Database Schema Analyzer: "
            + RESET
            + "Compare database schemas from multiple sources and generate Excel reports\n"
        )
        print(
            YELLOW
            + "2. Stored Procedure Analyzer: "
            + RESET
            + "Compare Stored Procedures from multiple databases on your system and generate Excel report\n"
        )
        print(
            YELLOW
            + "3. Stored Procedure Comparator + Diff Generator: "
            + RESET
            + "Compare Stored Procedures between two databases on your system, generate Excel reports, and store differential files in HTML format for visualizing differences\n"
        )

        choice = int(input("Enter your choice: \t"))
        print(f"You have selected option: {choice}.\n")

        if choice == 1:
            app1()
        elif choice == 2:
            app2()
        elif choice == 3:
            app3()
        else:
            print(
                RED + "Error: " + RESET + "Please select a valid choice from 1 to 4\n"
            )
    except ValueError:
        print(RED + "\nError: " + RESET + "Please enter a valid numeric choice.\n")
    except Exception as e:
        print(f"Error occurred: {str(e)}")


if __name__ == "__main__":
    main()
