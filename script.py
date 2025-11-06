import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import hashlib

def get_database_url(env="Local", database = None) -> str:
    """Get database URL based on environment."""
    return database

def add_unique_id(df) -> pd.DataFrame:
    """Add a unique ID column to the DataFrame based on row values."""
    df = df.copy()
    unique_values = df.astype(str).agg("|".join, axis=1).apply(
        lambda x: hashlib.sha256(x.encode("utf-8")).hexdigest()
    )
    df.insert(0, "ID", unique_values)
    return df

def new_transfers_only(df, engine) -> pd.DataFrame:
    """Filter DataFrame to only include new transfers not already in the database."""
    ensure_table_exists(df, engine, table_name="transfertable")
    existing_ids = pd.read_sql('SELECT "ID" FROM transfertable', engine)["ID"].tolist()
    new_transfers_df = df[~df["ID"].isin(existing_ids)].reset_index(drop=True)
    return new_transfers_df

def overlap(df, engine) -> bool:
    """Return True if any transfer IDs in df already exist in the database."""
    ensure_table_exists(df, engine, table_name="transfertable")
    existing_ids = pd.read_sql('SELECT "ID" FROM transfertable', engine)["ID"].tolist()
    overlap_exists = df["ID"].isin(existing_ids).any()
    return overlap_exists

def get_league_data(league_id, league_name, domain, transfer_page, engine, debug = False, insert_type = "append") -> pd.DataFrame:
    """Fetch and process data for a specific league."""
    df = pd.DataFrame()
    page = 1
    last_page = 1 if debug else 999
    retry_count = 0
    overlapToggle = False
    while page <= last_page:
        try:
            html = requests.get(f"{transfer_page}{league_id}/{page}").text
            soup = BeautifulSoup(html, "html.parser")
            table = soup.find("table")
            retry_count = 0
        except Exception as e:
            if retry_count < 3:
                retry_count += 1
                time.sleep(0.2)
                continue
            else:
                print(f"Failed to retrieve data for league {league_name} on page {page}: {e}")
                return df
                
        if page != last_page:
            try:
                last_page = int(table.find("li", class_="page-item last").find("a").get("href").strip("/").split("/")[-1])
            except:
                print(f"Could not find last page for league {league_name}, assuming single page.")
                last_page = page
    
        temp_df = pd.read_html(StringIO(str(table)))[0]
        player_links = [domain + tr.find_all("td")[1].find("a").get("href") for tr in table.find_all("tr")[1:]]
        
        temp_df.insert(0, "LEAGUE", league_name)
        temp_df.insert(3, "PLAYER_LINK", player_links)
        temp_df["Date"] = pd.to_datetime(temp_df["Date"], format="%d.%m.%Y / %H:%M")
        temp_df = add_unique_id(temp_df)
        process_dataframe(temp_df)
        if overlap(temp_df, engine) and insert_type == "append":
            print(f"Overlap found in league {league_name} on page {page}.")
            temp_df = new_transfers_only(temp_df, engine)
            overlapToggle = True
        df = pd.concat([df, temp_df], ignore_index=True)
        if overlapToggle:
            break

        page += 1
    
    return df

def process_dataframe(df) -> pd.DataFrame:
    """Process and clean the DataFrame."""
    df.drop("Transfer Type", axis=1, inplace=True)
    df.rename(columns={
        "Contract Type": "CONTRACT_TYPE",
        'Player': 'PLAYER',
        'Date': 'DATE',
        'Club': 'CLUB',
    }, inplace=True)
    return df

def save_to_database(df: pd.DataFrame, engine, table_name = "transfertable", insert_type = "append") -> None:
    """Save DataFrame to database."""
    df.to_sql(table_name, engine, if_exists=insert_type, index=False)

def ensure_table_exists(df, engine, table_name="transfertable"):
    try:
        df.head(0).to_sql(table_name, engine, if_exists="fail", index=False)
    except ValueError:
        # table already exists
        pass

def transfer_table_creator(debug = False, table_name = "transfertable", insert_type="append", ingestion=False, env="Local", database = None) -> pd.DataFrame:
    """Main function to orchestrate the transfer table creation process."""
    # Initialize constants
    domain = "https://www.emajorleague.com"
    transfer_page = domain + "/tournaments/league_transfers/"
    leagues = {
        33: "FC 26 | 1. Lig",
        34: "FC 26 | 2. Lig",
        35: "FC 26 | 3. Lig",
        36: "FC 26 | 4. Lig"
    }
    
    # Get database connection
    db_url = get_database_url(env, database)
    engine = create_engine(db_url) if ingestion else None
    
    # Fetch and combine data from all leagues
    all_data = pd.DataFrame()
    for league_id, league_name in leagues.items():
        league_df = get_league_data(league_id, league_name, domain, transfer_page, engine, debug, insert_type)
        all_data = pd.concat([all_data, league_df], ignore_index=True)

    # Save to database if required
    if ingestion and engine is not None:
        save_to_database(all_data, engine, table_name= 'transfertable', insert_type = insert_type)
    
    return all_data