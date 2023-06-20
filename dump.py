from sqlalchemy import create_engine, text
import pandas as pd
import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector


def execute_query_and_save_to_csv(filename, query, engine):
    offset = 0
    limit = 100
    total_rows_fetched = 0

    try:
        with engine.connect() as connection:
            while True:
                modified_query = query + f" LIMIT {limit} OFFSET {offset}"
                print(f"Executing query: {modified_query}")

                result = connection.execute(text(modified_query))
                rows = result.fetchall()

                if not rows:
                    break

                rows_fetched = len(rows)
                total_rows_fetched += rows_fetched
                print(f"Fetched {rows_fetched} rows, total: {total_rows_fetched}")

                df = pd.DataFrame(rows, columns=result.keys())

                if offset == 0:  # write headers on first run
                    df.to_csv(filename, mode='w', index=False, escapechar='\\')
                else:
                    df.to_csv(filename, mode='a', header=False, index=False, escapechar='\\')

                offset += limit

    except Exception as e:
        print(f"Error executing query: {e}")


def upload_to_google_sheets(filename):
    # Google Sheets credentials
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('cred.json',
                                                                   scope)  # Replace 'credentials.json' with your credentials file

    # Authenticate and open the Google Sheets document
    gc = gspread.authorize(credentials)
    spreadsheet = gc.open_by_url(
        'sheet.url')  # Replace with your Google Sheets URL


    # Create a new sheet and upload the data
    # worksheet = spreadsheet.add_worksheet(title='New Sheet', rows=1,
    #                                       cols=1)  # Replace 'New Sheet' with your desired sheet name

    # Get the current date and month
    current_date = datetime.now()
    sheet_name = current_date.strftime("%Y-%m")  # Format: YYYY-MM

    # Check if the sheet already exists
    existing_sheets = [sheet.title for sheet in spreadsheet.worksheets()]
    if sheet_name not in existing_sheets:
        # Create a new sheet
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=1)



    with open(filename, 'r') as file:
        content = file.read()
        data = [row.split(',') for row in content.split('\n') if
                row]  # Assuming your CSV file has comma-separated values

    worksheet.update('A1', data)


def main():
    try:
        # establish a database connection
        engine = create_engine(
            "mysql+mysqlconnector://username:password@serverurl/database name")

        input_folder = 'input'
        output_folder = 'output'
        current_date = datetime.now().strftime('%Y%m%d')

        for filename in os.listdir(input_folder):
            if filename.endswith('.sql'):
                with open(os.path.join(input_folder, filename), 'r') as file:
                    query = file.read().replace('\n', ' ')

                output_filename = os.path.join(output_folder, filename.replace('.sql', f'_{current_date}.csv'))
                print(f"\nProcessing file: {filename}")
                execute_query_and_save_to_csv(output_filename, query, engine)
                upload_to_google_sheets(output_filename)

    except Exception as e:
        print("An unexpected error occurred:", e)


if __name__ == "__main__":
    main()
