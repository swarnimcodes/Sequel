import os
import signal
import datetime
from os.path import exists
import re

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
import pandas as pd
from tqdm import tqdm

# ANSI escape codes for text colors
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RESET = "\033[0m"

# trying to implement pyodbc connection function just once to simplify codebase
def connection(server, database, username, password):
    return cursor


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
        print("Schema query execution" + GREEN +  " completed" + RESET + ".\n")
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
        raise Exception("An" + RED + " unexpected error " + RESET +  f"occurred: {str(ex)}")


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

        source_info["server"] = input("Enter" + GREEN + " Server " + RESET +  "Address:\t")
        source_info["database"] = input("Enter" + GREEN +  " Database " + RESET + "Name:\t")
        source_info["username"] = input("Enter Server" + GREEN + " Username:\t" + RESET)
        source_info["password"] = input("Enter Server" + GREEN +  " Password:\t" + RESET)

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
                    "Source database details are" + RED + " incomplete" + RESET + ". Please provide all required information."
            )

        print("Fetching Source Information... \n")

        try:
            source_schema = fetch_schema(
                source_server, source_database, source_username, source_password
            )  # has information regarding source schema
        except Exception as e:
            raise ValueError(
                f"Couldn't fetch source schema. Perhaps the" + RED + " login details " + RESET + "or" + RED + " database details " + RESET + "you entered are incorrect?\n"
            )

        # Create a single workbook outside the loop
        workbook = openpyxl.Workbook()

        number_of_target_db = int(
            input("How many databases do you want to compare against source? \t")
        )
        nested_target_dbs = {}

        if number_of_target_db <= 0:
            print("No databases to compare." + RED + " Exiting." + RESET)
            return

        # Numbering should be from 1 to n not countdown
        while number_of_target_db > 0:
            # Initialize the nested dictionary for the current target DB number
            nested_target_dbs[number_of_target_db] = {}
            nested_target_dbs[number_of_target_db]["server"] = input(
                f"\nEnter Server Address for Target DB number {number_of_target_db}: "
            )
            nested_target_dbs[number_of_target_db]["database"] = input(
                f"Enter Database Name for Target DB number {number_of_target_db}: "
            )
            nested_target_dbs[number_of_target_db]["username"] = input(
                f"Enter Username for Target DB number {number_of_target_db}: "
            )
            nested_target_dbs[number_of_target_db]["password"] = input(
                f"Enter Password for Target DB number {number_of_target_db}: "
            )

            target_server = nested_target_dbs[number_of_target_db]["server"]
            target_database = nested_target_dbs[number_of_target_db]["database"]
            target_username = nested_target_dbs[number_of_target_db]["username"]
            target_password = nested_target_dbs[number_of_target_db]["password"]

            if (
                not target_server
                or not target_database
                or not target_username
                or not target_password
            ):
                print(
                    "Target database number {number_of_target_db}'s details are incomplete. Skipping comparison for this target."
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
                        nested_target_dbs[number_of_target_db]["database"],
                    )
                except Exception as e:
                    print(
                        f"Error fetching schema for target database number {number_of_target_db}: {str(e)}"
                    )

            number_of_target_db = number_of_target_db - 1

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
        excel_file_name = f"Schema_Comparison_Report_{timestamp}.xlsx"

        workbook.remove(workbook.active)
        workbook.save(excel_file_name)
        print(
            f"\nExcel file successfully created at {os.path.abspath(excel_file_name)}\n\n"
        )
        os.system(f'explorer /select,"{os.path.abspath(excel_file_name)}"')
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
    stripped_sql_file = re.sub(r"--.*", "", sql_file_contents)
    stripped_sql_file = re.sub(r"/\*.*?\*/", "", stripped_sql_file, flags=re.DOTALL)
    stripped_sql_file = "\n".join(
        " ".join(part.strip() for part in line.split() if part.strip())
        for line in stripped_sql_file.splitlines()
        if line.strip()
    )
    return stripped_sql_file


def difference(source_sql_path, test_sql_path) -> bool:
    source_contents = open(source_sql_path, "r").read().upper()
    test_contents = open(test_sql_path, "r").read().upper()

    stripped_sql_file_source = strip_comments(source_contents)
    stripped_sql_file_test = strip_comments(test_contents)

    if stripped_sql_file_source == stripped_sql_file_test:
        return True
    else:
        return False


def app2():
    print("Enter details of your SOURCE database: \n")
    source_db_dir = input("Enter the SOURCE database directory location: \n")

    num_target_dbs = int(
        input("Enter number of TARGET databases you want to compare: \n")
    )

    target_db_dirs = []

    while num_target_dbs > 0:
        target_db_dirs.append(
            input(f"Enter location for target database number {num_target_dbs}: ")
        )
        num_target_dbs = num_target_dbs - 1

    sp_data = []

    # Get the list of sql file in source database
    source_sql_file_list = os.listdir(source_db_dir)

    for sql_file in source_sql_file_list:
        sp_name = sql_file[:-4]  # Remove the .sql extension
        sp_info = {"SP Name": sp_name}
        source_sql_path = os.path.join(source_db_dir, sql_file)

        #
        for target_db_dir in target_db_dirs:
            target_sql_path = os.path.join(target_db_dir, sql_file)
            if os.path.exists(target_sql_path):
                if difference(source_sql_path, target_sql_path):
                    sp_info[os.path.basename(target_db_dir)] = "PRESENT & EQUAL"
                else:
                    sp_info[os.path.basename(target_db_dir)] = "PRESENT & UNEQUAL"

            else:
                sp_info[os.path.basename(target_db_dir)] = "ABSENT"

        sp_data.append(sp_info)

    df = pd.DataFrame(sp_data)

    output_excel_path = input(
        "Name the excel file: "
    )  # Make it so that the excel file is created automatically
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
    # Open the folder instead
    os.system(f'start excel "{output_excel_path}"')


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
    print("app3\n")
    comparison_results = []

    # No need for this. store the diffs in a folder from where the program is run
    output_dir = input(
        "Enter the location of directory where you want to store the DIFF HTML files: \n"
    )
    excel_output_file = input("Name the excel file: ")

    # Source Database
    folder1_path = input("Enter location of the SOURCE database directory: \n")

    # Database to compare with source
    folder2_path = input("Enter location of the TARGET database directory: \n")

    # Define cell fill colors
    different_color = PatternFill(
        start_color="8ab4ff", end_color="8ab4ff", fill_type="solid"
    )
    missing_color = PatternFill(
        start_color="ffd6ca", end_color="ffd6ca", fill_type="solid"
    )

    nltk.download("punkt")  # No output should be thrown on terminal when downloading
    nltk.download("words")
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
                file1_contents = open(file1_path, "r").read().upper()
                normalized_sql_1 = normalize_sql(file1_contents)
                file1_nocomments = strip_sql_comments(normalized_sql_1)

                file2_contents = open(file2_path, "r").read().upper()
                normalized_sql_2 = normalize_sql(file2_contents)
                file2_nocomments = strip_sql_comments(normalized_sql_2)

                if file1_nocomments == file2_nocomments:
                    content_comparison = "Equal"
                    print(f"Files {sql_file} are equal")
                else:
                    content_comparison = "Different"
                    print(f"Files {sql_file} are unequal")
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
    #
    # df = pd.DataFrame(
    #     comparison_results,
    #     columns=[
    #         "SP Name",
    #         f"Present in {os.path.dirname(folder1_path)}",
    #         f"Present in {os.path.dirname(folder2_path)}",
    #         "Content Comparison",
    #         "Diff File",
    #     ],
    # )
    # df.to_excel(excel_output_file, index=False)
    # os.system(f'start excel "{excel_output_file}"')

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

    # Save the workbook
    wb.save(excel_output_file)
    # Open the folder insted of the excel file
    os.system(f'start excel "{excel_output_file}"')


# END OF APP 3 #########################################################################

# Convert all sql files to utf-8 by default and remove app 4


def convert_utf16_to_utf8(input_file_path, output_file_path):
    try:
        with open(input_file_path, "r", encoding="utf-16") as utf16_file:
            utf16_content = utf16_file.read()

        with open(output_file_path, "w", encoding="utf-8") as utf8_file:
            utf8_file.write(utf16_content)

        print(f"Conversion successful: {input_file_path} -> {output_file_path}")
    except FileNotFoundError:
        print(f"File not found: {input_file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")


def batch_convert_utf16_to_utf8(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    input_files = [f for f in os.listdir(input_dir)]

    for input_file in input_files:
        input_file_path = os.path.join(input_dir, input_file)
        output_file_path = os.path.join(output_dir, input_file)
        convert_utf16_to_utf8(input_file_path, output_file_path)


def app4():
    input_dir = input("Enter directory with utf-16 files: \n")
    output_dir = input("Enter directory where you want to store the utf-8 files: \n")

    batch_convert_utf16_to_utf8(input_dir, output_dir)


# END OF APP 4 #########################################################################


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
        print(YELLOW + "4: UTF-16 to UTF-8 File Converter: " + RESET +
              "Convert UTF-16 files to UTF-8 files\n")
        choice = int(input("Enter your choice: \t"))
        print(f"You have selected option: {choice}.\n")

        if choice == 1:
            app1()
        elif choice == 2:
            app2()
        elif choice == 3:
            app3()
        elif choice == 4:
            app4()
        else:
            print("Please select a valid choice from 1 to 4")
    except ValueError:
        print("Please enter a valid numeric choice.")
    except Exception as e:
        print(f"Error occurred: {str(e)}")


if __name__ == "__main__":
    main()
