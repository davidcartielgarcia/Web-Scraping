# Import required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

# Function to log messages with a timestamp
def log_progress(message):
    now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    with open("code_log.txt", "a") as file:
        file.write(f"{now} : {message}\n")

# Extract data from a webpage and format it into a DataFrame
def extract(url, table_attribs):
    df = pd.DataFrame(columns=table_attribs)
    html = requests.get(url).text
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find_all("tbody")[0]
    rows = table.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        if cols:
            name = cols[1].text.strip()
            val = float(cols[2].text.strip())
            df = pd.concat([df, pd.DataFrame([[name, val]], columns=table_attribs)], ignore_index=True)
    return df

# Perform data transformations, including currency conversions
def transform(df, csv_path):
    df_rate = pd.read_csv(csv_path)
    rates = {row[0]: row[1] for row in df_rate.itertuples(index=False)}
    df["MC_GBP_Billion"] = [np.round(x * rates['GBP'], 2) for x in df["MC_USD_Billion"]]
    df["MC_EUR_Billion"] = [np.round(x * rates['EUR'], 2) for x in df["MC_USD_Billion"]]
    df["MC_INR_Billion"] = [np.round(x * rates['INR'], 2) for x in df["MC_USD_Billion"]]
    return df

# Save DataFrame to a CSV file
def load_to_csv(df, output_path):
    df.to_csv(output_path)

# Save DataFrame to a SQLite database table
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists="replace")

# Execute an SQL query and display the result
def run_query(query_statement, sql_connection):
    output = pd.read_sql(query_statement, sql_connection)
    print(f"Query: {query_statement}")
    print(output)

# Main script: ETL process
url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attr = ["Name", "MC_USD_Billion"]
csv_path = "Largest_banks_data.csv"
db_name = "Banks.db"
db_table = "Largest_banks"

log_progress("Preliminaries complete. Initiating ETL process")
df = extract(url, table_attr)
log_progress("Data extraction complete. Initiating Transformation process")
df = transform(df, "exchange_rate.csv")
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df, csv_path)
log_progress("Data saved to CSV file")

conn = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

load_to_db(df, conn, db_table)
log_progress("Data loaded to Database as a table, Executing queries")

run_query("SELECT * FROM Largest_banks", conn)
log_progress("Query 1 executed")

run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", conn)
log_progress("Query 2 executed")

run_query("SELECT Name FROM Largest_banks LIMIT 5", conn)
log_progress("Query 3 executed")

conn.close()
log_progress("Server connection closed")
