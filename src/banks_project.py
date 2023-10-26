"""
Simple ETL project to extract data from a website, transform it and load it to a database.
"""

import logging
import sqlite3
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_PATH = Path(__file__).parent.parent


def log_progress(message: str) -> None:
    """
    This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing.
    """
    log_format = "%(asctime)s : %(message)s"
    logging.basicConfig(filename="code_log.txt", level=logging.INFO, format=log_format)
    logging.info(message)


def extract(
    url: str,
    table_attribs: dict | None = None,
) -> pd.DataFrame:
    """
    This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing.
    """
    if not table_attribs:
        table_attribs = {"class": "wikitable"}

    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", attrs=table_attribs)
    log_progress("Data extraction complete. Initiating Transformation process")
    return pd.read_html(str(table))[0]


def transform(
    df: pd.DataFrame,
    csv_path: Path | None = None,
) -> pd.DataFrame:
    """
    This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies.
    """
    if not csv_path:
        csv_path = BASE_PATH / "data" / "exchange_rate.csv"

    with open(csv_path, "r") as f:
        exchange_rates = pd.read_csv(f)

    rates_dict = exchange_rates.set_index("Currency").to_dict()["Rate"]

    df = df.assign(
        MC_GBP_Billion=lambda x: round(x["Market cap (US$ billion)"] * rates_dict["GBP"], 2),
        MC_EUR_Billion=lambda x: round(x["Market cap (US$ billion)"] * rates_dict["EUR"], 2),
        MC_INR_Billion=lambda x: round(x["Market cap (US$ billion)"] * rates_dict["INR"], 2),
    )
    log_progress("Data transformation complete. Initiating Loading process")
    return df


def load_to_csv(
    df: pd.DataFrame,
    output_path: Path | None = None,
) -> None:
    """
    This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.
    """
    if not output_path:
        output_path = BASE_PATH / "data" / "final_data.csv"

    df.to_csv(output_path, index=False)
    log_progress("Data saved to CSV file")


def load_to_db(
    df: pd.DataFrame,
    sql_connection: sqlite3.Connection | None = None,
    table_name: str | None = None,
) -> None:
    """
    This function saves the final data frame to a database
    table with the provided name. Function returns nothing.
    """
    if not table_name:
        table_name = "Largest_banks"

    if not sql_connection:
        sql_connection = sqlite3.connect("Banks.db")

    log_progress("SQL Connection initiated")
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)
    log_progress("Data loaded to Database as a table, Executing queries")


def run_query(
    query_statement: str,
    sql_connection: sqlite3.Connection | None = None,
) -> None:
    """
    This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing.
    """
    if not sql_connection:
        sql_connection = sqlite3.connect("Banks.db")
    
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    query_result = cursor.fetchall()
    
    print(f"Query Statement: {query_statement}")
    print(f"Query Result: {query_result}\n")
    

if __name__ == "__main__":
    """
    Here, you define the required entities and call the relevant
    functions in the correct order to complete the project. Note that this
    portion is not inside any function.
    """
    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    sql_connection = sqlite3.connect("Banks.db")
    log_progress("Preliminaries complete. Initiating ETL process")

    # Extract, Transform and Load
    df = extract(url)
    df = transform(df)
    load_to_csv(df)
    load_to_db(df)
    
    # Query 1: Print the contents of the entire table
    query_1 = "SELECT * FROM Largest_banks"
    run_query(query_1, sql_connection)

    # Query 2: Print the average market capitalization of all the banks in Billion USD
    query_2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
    run_query(query_2, sql_connection)
    
    # Query 3: Print only the names of the top 5 banks
    query_3 = 'SELECT "Bank name" from Largest_banks LIMIT 5'
    run_query(query_3, sql_connection)
    
    # Query 4: Print the schema of the table
    query_4 = "PRAGMA table_info(Largest_banks);"
    run_query(query_4, sql_connection)
    
    log_progress("Process Complete")
    sql_connection.close()
    log_progress("Server Connection closed")