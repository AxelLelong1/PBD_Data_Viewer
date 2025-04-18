import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import sklearn
import glob
import time
import re
import mylogging
import os
import timescaledb_model as tsdb

TSDB = tsdb.TimescaleStockMarketModel
HOME = "/home/bourse/data/"   # we expect subdirectories boursorama and euronext

#=================================================
# Extract, Transform and Load data in the database
#=================================================

#
# private functions
# 

#
# decorator
# 

def timer_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} run in {(end_time - start_time):.2f} seconds.")
        return result

    return wrapper

#
# public functions
# 

def daterange(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)

def find_boursorama(date):
    d = date.strftime("%Y-%m-%d")
    base_path = os.path.join(HOME, "boursorama")
    matching_files = []

    # Parcours récursif
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if d in file:
                full_path = os.path.join(root, file)
                matching_files.append(full_path)

    if not matching_files:
        print(f"Aucun fichier Boursorama trouvé pour la date {d}.")
        return None

    return matching_files

def read_boursorama(path):
    try:
        df = pd.read_pickle(path)
        headers = df.columns.tolist()
        return df, headers
    except Exception as e:
        print(f"Erreur lors de la lecture de {path}: {e}")
        return None, None


def find_euronext(date):
    date = date.isoformat()
    path = os.path.join(HOME, "euronext", f"Euronext_Equities_{date}.csv")
    if not os.path.exists(path):
        print(f"Fichier Euronext {path} introuvable.")
        return None
    return path

def read_euronext(path):
    try:
        df = pd.read_csv(path, sep='\t', skiprows=[1,2,3])
        headers = df.columns.tolist()
        return df, headers
    except Exception as e:
        print(f"Erreur lors de la lecture de {path}: {e}")
        return None, None

def insert_euronext(df, db:TSDB, path):
    try:
        # Récupération des marchés
        markets = pd.DataFrame()
        markets['name'] = df['Name']
        markets['alias'] = None
        markets['boursorama'] = None
        markets['sws'] = None
        markets['euronext'] = df['Market']
        
        # Récupération des entreprises
        companies = pd.DataFrame()
        companies["name"] = df['Name']
        companies["mid"] = None #stands for Market Id --  m_id -- mid
        companies["symbol"] = df['Symbol']
        companies["isin"] = df["ISIN"]
        companies["boursorama"] = None
        companies["euronext"] = df["Market"]
        companies["pea"] = False
        companies["sector1"] = None
        companies["sector2"] = None
        companies["sector3"] = None

        # Récupération des daystocks
        daystocks = pd.DataFrame()
        daystocks["date"] = pd.to_datetime(df["Last Date/Time"], format="%d/%m/%y %H:%M", errors="coerce")
        daystocks.dropna(subset=["date"], inplace=True)

        daystocks["cid"] = None
        daystocks["open"] = pd.to_numeric(df["Open"].replace("-", pd.NA), errors="coerce")
        daystocks["close"] = pd.to_numeric(df["Last"].replace("-", pd.NA), errors="coerce")
        daystocks["high"] = pd.to_numeric(df["High"].replace("-", pd.NA), errors="coerce")
        daystocks["low"] = pd.to_numeric(df["Low"].replace("-", pd.NA), errors="coerce")
        daystocks["volume"] = pd.to_numeric(df["Volume"].replace("-", pd.NA), errors="coerce")
        daystocks["mean"] = pd.to_numeric(df["Turnover"].replace("-", pd.NA), errors="coerce") / daystocks["volume"]
        daystocks["std"] = daystocks[["open", "high", "low", "close"]].std(axis=1)
        daystocks["isin"] = df["ISIN"]
        daystocks["euronext"] = df["Market"]

        #-------------------------------------------------------------------------------------------
        # Adding market
        existing_markets = db.df_query("SELECT name, euronext FROM markets")

        # Filtrer uniquement les nouveaux marchés
        new_markets = markets[~markets.set_index(['name', 'euronext']) #Set name and euronext as index (a company can have multiple euronext)
                                .index.isin(existing_markets.set_index(['name', 'euronext']).index)] #Return a table of bool showing the presence 

        if not new_markets.empty:
            try:
                db.df_write(new_markets, 'markets')
            except Exception as e:
                print("Erreur lors de l'insertion des marchés:", e)
        else:
            print("Aucun nouveau marché à insérer.")
        
        #-------------------------------------------------------------------------------------------
        # Adding companies

        # Associer chaque entreprise à l'ID du marché correspondant
        existing_markets = db.df_query("SELECT id, name, euronext FROM markets")

        # Mapper les marchés existants pour obtenir les IDs
        market_map = existing_markets.set_index(['name', 'euronext'])['id'].to_dict()
        companies['mid'] = companies.apply(lambda row: market_map.get((row['name'], row['euronext'])), axis=1)
        
        # Insérer uniquement les nouvelles sociétés
        existing_companies = db.df_query("SELECT isin FROM companies")
        new_companies = companies[~companies.set_index('isin').index.isin(existing_companies.set_index('isin').index)]

        if not new_companies.empty and new_companies["mid"].notna().all():
            try:
                db.df_write(new_companies, 'companies')
            except Exception as e:
                print("Erreur lors de l'insertion des companies:", e)
        else:
            print("Aucune nouvelle companie à ajouter.")

        #-------------------------------------------------------------------------------------------
        # Adding daystocks
        company_id = db.df_query("SELECT id, isin, euronext FROM companies")

        company_id_map = company_id.set_index(['isin', 'euronext'])['id'].to_dict()
        daystocks['cid'] = daystocks.apply(lambda row: company_id_map.get((row['isin'], row['euronext'])), axis=1)
        daystocks["cid"] = daystocks["cid"].astype("Int64")

        daystocks.drop(columns=["isin", "euronext"], inplace=True)

        daystocks["date"] = pd.to_datetime(df["Last Date/Time"], format="%d/%m/%y %H:%M", errors="coerce")
        tz_map = {
            "CET": "Europe/Paris",
            "CEST": "Europe/Paris",
            "UTC": "UTC",
            "GMT": "Etc/GMT"
        }

        df["tz_full"] = df["Time Zone"].map(tz_map).fillna("Europe/Paris")

        daystocks["date"] = [
            pd.Timestamp(dt).tz_localize(tz)
            for dt, tz in zip(daystocks["date"], df["tz_full"])
        ]

        daystocks.drop(columns=["tz_full"], inplace=True, errors="ignore")
        if not daystocks.empty and daystocks["cid"].notna().all():
            try:
                db.df_write(daystocks, 'daystocks')
            except Exception as e:
                print("Erreur lors de l'insertion des stocks jounaliers:", e)

        print(f"Fichier Euronext {path} indexé avec succès.")
    except Exception as e:
            print(f"Erreur SQL avec {path}: {e}")
            #db.connection.rollback()

@timer_decorator
def store_files(start:str, end:str, website:str, db:TSDB):
    for date in daterange(datetime.strptime(start, "%Y-%m-%d").date(), datetime.strptime(end, "%Y-%m-%d").date()):
        
        df = pd.DataFrame()
        headers = []

        if website == "euronext":
            path = find_euronext(date)
            if path == None: #coudln't read file
                continue

            df, headers = read_euronext(path)
            if headers == None:
                continue

            insert_euronext(df, db, path)

        if website == "boursorama":
            files = find_boursorama(date)
            if files == None or files == []:
                continue

            files.sort()
            for file in files:
                df, headers = read_boursorama(file)
                print(headers)
                print(df.head)
                print()
            
        try:
            print("test")
            # Récupération des stocks journaliers
            #stocks = df[['Last Date/Time', 'ISIN', 'Last', 'Volume']].dropna()
            #stocks = stocks.rename(columns={'Last Date/Time': 'date', 'Last': 'value', 'Volume': 'volume'})
            #stocks['date'] = pd.to_datetime(stocks['date'])
            #stocks['cid'] = stocks['ISIN'].map(lambda x: db.df_query(f"SELECT id FROM companies WHERE isin='{x}'").values[0][0] if x in companies['ISIN'].values else None)
            #stocks = stocks.drop(columns=['ISIN']).dropna()
            
            # Récupération des daystocks
            #daystocks = df[['Last Date/Time', 'ISIN', 'Open', 'High', 'Low', 'Last', 'Volume']].dropna()
            #daystocks = daystocks.rename(columns={'Last Date/Time': 'date', 'Last': 'close'})
            #daystocks['date'] = pd.to_datetime(daystocks['date'])
            #daystocks['cid'] = daystocks['ISIN'].map(lambda x: db.df_query(f"SELECT id FROM companies WHERE isin='{x}'").values[0][0] if x in companies['ISIN'].values else None)
            #daystocks['mean'] = (daystocks['High'] + daystocks['Low']) / 2
            #daystocks['std'] = None  # Peut être calculé plus tard
            #daystocks = daystocks.drop(columns=['ISIN']).dropna()
            
            #db.df_write(stocks, 'stocks', if_exists='append')
            #db.df_write(daystocks, 'daystocks', if_exists='append')

        except Exception as e:
            print(f"Erreur SQL avec {path}: {e}")
            #db.connection.rollback()

if __name__ == '__main__':
    print("Go Extract Transform and Load")
    pd.set_option('display.max_columns', None)  # usefull for dedugging
    db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
    db._purge_database()
    db._setup_database()
    #db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker
    store_files("2020-05-01", "2020-06-01", "euronext", db) # one month to test
    #store_files("2020-05-01", "2020-06-01", "boursorama", db) # one month to test
    print("Done Extract Transform and Load")