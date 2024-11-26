# Required libraries for web scraping, data processing, and logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

# Function to log progress messages with a timestamp
def log_progress(message):
    now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    with open("code_log.txt", "a") as file:
        file.write(f"{now} : {message}\n")

# Function to extract match dates and details from a web page
def extract_dates(url, teams, table_att):
    df = pd.DataFrame(columns=table_att)  # Initialize an empty DataFrame
    html = requests.get(url).text  # Fetch the web page content
    soup = BeautifulSoup(html, "html.parser")  # Parse the HTML content
    jornadas = soup.find_all("table")  # Find all match tables on the page
    for jornada in jornadas:
        findesemana = jornada.find("thead").text.strip().split()  # Get the match day header
        table = jornada.find("tbody")  # Access the table body
        rows = table.find_all("tr")  # Find all rows in the table
        for row in rows:
            cols = row.find_all("td")  # Extract columns from each row
            dia = cols[0].text.strip().split("/")  # Parse the match date
            local = cols[1].text.strip()  # Get the home team
            visitant = cols[3].text.strip()  # Get the away team
            year = findesemana[0].split("-")[0]  # Extract the year
            hora = "" if len(findesemana) == 1 else cols[4].text.strip()  # Get the match time
            data = datetime(int(year), int(dia[1]), int(dia[0]))  # Format the date
            if local in teams and hora != "Fin":  # Filter matches involving specific teams
                data = data.strftime("%d/%m/%Y")
                # Append match details to the DataFrame
                df_r = pd.DataFrame(data=[[data, hora, local, visitant]], columns=table_att)
                df = pd.concat([df, df_r], ignore_index=True)
    return df

# Function to load abbreviations from a CSV file into a dictionary
def get_dic_csv(path_csv):
    df = pd.read_csv(path_csv)
    return {df.iloc[i, 0]: df.iloc[i, 1] for i in range(len(df))}

# Function to create a "Fixture" column based on team abbreviations
def create_Fitxure(df, abr):
    df["Fitxure"] = [abr[local].strip() + " v " + abr[visitant].strip() 
                     for local, visitant in zip(df["Local"], df["Visitant"])]
    return df

# Function to prepare a structured calendar table for export
def calendar_Table(df, sport, tournament, location, table_att):
    df_calendar = pd.DataFrame(columns=table_att)  # Initialize the calendar DataFrame
    df_calendar["Dia"] = df["Dia"]  # Add match date
    df_calendar["Hora"] = df["Hora"]  # Add match time
    df_calendar["Sport"] = sport  # Add sport type
    df_calendar["Tournament"] = tournament  # Add tournament name
    df_calendar["Fitxure"] = df["Fitxure"]  # Add fixture details
    # Map the location of the home team
    df_calendar["Location"] = [location[team] for team in df["Local"]]
    return df_calendar

# Function to export a DataFrame to a CSV file
def export_csv(df, csv_name):
    df.to_csv(csv_name)

# Main script parameters
url = "https://www.sport.es/resultados/futbol/primera-division/calendario-liga/"
local = ["Barcelona", "Espanyol", "Girona"]  # Teams of interest
table_attr = ["Dia", "Hora", "Local", "Visitant"]  # Match details columns
table_calendar = ["Dia", "Hora", "Sport", "Tournament", "Fitxure", "Location"]  # Calendar columns
sport = "Football (M)"  # Sport name
tournament = "La Liga"  # Tournament name
location = {"Barcelona": "Barcelona", "Espanyol": "Barcelona", "Girona": "Girona"}  # Team locations
csv_name = "calendar_LaLiga.csv"  # Output CSV file name

# Log progress and perform the ETL process
log_progress("Preliminaries complete. Initiating ETL process")

df_partits = extract_dates(url, local, table_attr)  # Extract match data
log_progress("Data extraction complete. Initiating Transformation process")

dic = get_dic_csv('24_25_LaLiga_teams.txt')  # Load team abbreviations
log_progress("Abbreviation teams from CSV done")

df_partits = create_Fitxure(df_partits, dic)  # Create fixture column
log_progress("Setting fixture attribute done")

df = calendar_Table(df_partits, sport, tournament, location, table_calendar)  # Prepare calendar table
log_progress("Creation of the calendar to be exported done")

export_csv(df, csv_name)  # Export the calendar to a CSV file
log_progress(f"CSV with name {csv_name} exported")
