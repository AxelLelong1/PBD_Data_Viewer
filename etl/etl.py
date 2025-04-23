import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sklearn
from zipfile import ZipFile
import glob
import time
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
        return None

    return matching_files

def read_boursorama(path):
    try:
        df = pd.read_pickle(path)
        return df
    except Exception as e:
        return None


def find_euronext(date):
    date = date.isoformat()
    path = os.path.join(HOME, "euronext", f"Euronext_Equities_{date}.csv")
    if not os.path.exists(path):
        path = os.path.join(HOME, "euronext", f"Euronext_Equities_{date}.xlsx")
        if not os.path.exists(path):
            print(f"Fichier Euronext {date} introuvable.")
            return None
    return path

def read_euronext(path):
    try:
        df = pd.DataFrame()
        if (path.split('.')[-1] == "csv"):
            df = pd.read_csv(path, sep='\t', skiprows=[1,2,3])
        else:
            df = pd.read_excel(path, skiprows=[1,2,3])
        return df
    except Exception as e:
        print(e)
        return None
    
def get_euronext_date(path):
    date = path.split('_')[-1].split('.')[0]
    res = datetime.fromisoformat(date)
    return res 

def insert_euronext_csv(df, db:TSDB, path):
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
        
        daystocks["cid"] = None
        daystocks["open"] = pd.to_numeric(df["Open"].replace("-", pd.NA), errors="coerce")
        daystocks["close"] = pd.to_numeric(df["Last"].replace("-", pd.NA), errors="coerce")
        daystocks["high"] = pd.to_numeric(df["High"].replace("-", pd.NA), errors="coerce")
        daystocks["low"] = pd.to_numeric(df["Low"].replace("-", pd.NA), errors="coerce")
        daystocks["volume"] = pd.to_numeric(df["Volume"].replace("-", pd.NA), errors="coerce")
        daystocks["mean"] = pd.to_numeric(df["Turnover"].replace("-", pd.NA), errors="coerce") / daystocks["volume"]
        daystocks["std"] = daystocks[["open", "high", "low", "close"]].std(axis=1)
        daystocks["euronext"] = df["Market"]
        daystocks["name"] = df["Name"]
        daystocks["date"] = get_euronext_date(path)

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

        #-------------------------------------------------------------------------------------------
        # Adding companies

        # Associer chaque entreprise à l'ID du marché correspondant
        existing_markets = db.df_query("SELECT id, name, euronext FROM markets")

        # Insérer uniquement les nouvelles sociétés
        existing_companies = db.df_query("SELECT name, euronext FROM companies")

        # Filtre les nouvelles sociétés basées sur (name, euronext)
        companies = companies[~companies[['name', 'euronext']].apply(tuple, axis=1).isin(
            existing_companies[['name', 'euronext']].apply(tuple, axis=1))]

        # Mapper les marchés existants pour obtenir les IDs
        market_map = existing_markets.set_index(['name', 'euronext'])['id'].to_dict()
        companies['mid'] = companies.apply(lambda row: market_map.get((row['name'], row['euronext'])), axis=1)
        companies['mid'] = companies['mid'].astype("Int64")
        
        if not companies.empty:
            try:
                db.df_write(companies, 'companies')
            except Exception as e:
                print("Erreur lors de l'insertion des companies:", e)

        #-------------------------------------------------------------------------------------------
        # Adding daystocks
        company_id = db.df_query("SELECT id, name, euronext FROM companies")

        company_id_map = company_id.set_index(['name', 'euronext'])['id'].to_dict()
        daystocks['cid'] = daystocks.apply(lambda row: company_id_map.get((row['name'], row['euronext'])), axis=1)
        daystocks["cid"] = daystocks["cid"].astype("Int64")

        null_cid_rows = daystocks[daystocks['cid'].isna()]
        print("Lignes avec CID nul :")
        print(null_cid_rows)
        #print(company_id_map.get(("FR0013529815", "Euronext Paris")))

        daystocks.drop(columns=["euronext", "name"], inplace=True)

        if not daystocks.empty and daystocks["cid"].notna().all():
            try:
                db.df_write(daystocks, 'daystocks')
            except Exception as e:
                print("Erreur lors de l'insertion des stocks jounaliers:", e)

        print(path, " indexé")
    except Exception as e:
            print(f"Erreur SQL avec {path}: {e}")
            #db.connection.rollback()

def insert_euronext_xlsx(df, db:TSDB, path):
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
        
        daystocks["cid"] = None
        daystocks["open"] = pd.to_numeric(df["Open Price"].replace("-", pd.NA), errors="coerce")
        daystocks["close"] = pd.to_numeric(df["last Price"].replace("-", pd.NA), errors="coerce")
        daystocks["high"] = pd.to_numeric(df["High Price"].replace("-", pd.NA), errors="coerce")
        daystocks["low"] = pd.to_numeric(df["low Price"].replace("-", pd.NA), errors="coerce")
        daystocks["volume"] = pd.to_numeric(df["Volume"].replace("-", pd.NA), errors="coerce")
        daystocks["mean"] = pd.to_numeric(df["Turnover"].replace("-", pd.NA), errors="coerce") / daystocks["volume"]
        daystocks["std"] = daystocks[["open", "high", "low", "close"]].std(axis=1)
        daystocks["name"] = df["Name"]
        daystocks["euronext"] = df["Market"]
        daystocks["date"] = get_euronext_date(path)

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

        #-------------------------------------------------------------------------------------------
        # Adding companies

        # Associer chaque entreprise à l'ID du marché correspondant
        existing_markets = db.df_query("SELECT id, name, euronext FROM markets")

        # Mapper les marchés existants pour obtenir les IDs
        market_map = existing_markets.set_index(['name', 'euronext'])['id'].to_dict()
        companies['mid'] = companies.apply(lambda row: market_map.get((row['name'], row['euronext'])), axis=1)
        
        # Insérer uniquement les nouvelles sociétés
        existing_companies = db.df_query("SELECT name, euronext FROM companies")

        # Filtre les nouvelles sociétés basées sur (name, euronext)
        new_companies = companies[~companies[['name', 'euronext']].apply(tuple, axis=1).isin(
            existing_companies[['name', 'euronext']].apply(tuple, axis=1))]

        if not new_companies.empty:
            try:
                db.df_write(new_companies, 'companies')
            except Exception as e:
                print("Erreur lors de l'insertion des companies:", e)

        #-------------------------------------------------------------------------------------------
        # Adding daystocks
        company_id = db.df_query("SELECT id, name, euronext FROM companies")

        company_id_map = company_id.set_index(['name', 'euronext'])['id'].to_dict()
        daystocks['cid'] = daystocks.apply(lambda row: company_id_map.get((row['name'], row['euronext'])), axis=1)
        daystocks["cid"] = daystocks["cid"].astype("Int64")

        daystocks.drop(columns=["name", "euronext"], inplace=True)

        if not daystocks.empty and daystocks["cid"].notna().all():
            try:
                db.df_write(daystocks, 'daystocks')
            except Exception as e:
                print("Erreur lors de l'insertion des stocks jounaliers:", e)

        print(path, " indexé")
    except Exception as e:
            print(f"Erreur SQL avec {path}: {e}")
            #db.connection.rollback()

def parse_price(val):
    if pd.isna(val):
        return None
    val = str(val).strip()
    if '(c)' in val:
        return float(val.replace('(c)', '').replace(' ', '').replace(',', '.').strip()) / 100
    elif '(s)' in val:
        return float(val.replace('(s)', '').replace(' ', '').replace(',', '.'))
    else:
        return float(val.replace(' ', '').replace(',', '.'))

def get_bousorama_date(path):
    # On découpe le chemin par les espaces
    filename = path.split("/")[-1]  # => "compB 2020-05-04 17:32:01.848062.bz2"
    parts = filename.split()          # => ["compB", "2020-05-04", "17:32:01.848062.bz2"]

    # On extrait la date et l'heure
    date_part = parts[1]              # "2020-05-04"
    time_part = parts[2].split(".bz2")[0]  # "17:32:01.848062"

    # Fusion et conversion en datetime
    timestamp_str = f"{date_part} {time_part}"
    timestamp = datetime.fromisoformat(timestamp_str)

    return timestamp

def insert_boursorama(df, db, path, company_id_map):
    stocks = pd.DataFrame()
    
    stocks['value'] =  df['last'].apply(parse_price)
    stocks['volume'] =  pd.to_numeric(df['volume'])
    stocks['date'] = get_bousorama_date(path)
    stocks['symbol'] = df['symbol']

    stocks['cid'] = stocks.apply(lambda row: company_id_map.get((row['symbol'])), axis=1)
    stocks["cid"] = stocks["cid"].astype("Int64")

    stocks.drop(columns=["symbol"], inplace=True)

    print(get_bousorama_date(path), " Bourso indexe")
    return stocks


@timer_decorator
def store_files(start:str, end:str, website:str, db:TSDB):
    stocks = []
    if (website == "boursorama"): # sera forcément demandé après indexation des companies
        company_id = db.df_query("SELECT id, symbol FROM companies")
        company_id_map = company_id.set_index(['symbol'])['id'].to_dict()

    for date in daterange(datetime.strptime(start, "%Y-%m-%d").date(), datetime.strptime(end, "%Y-%m-%d").date()):
        df = pd.DataFrame()

        if website == "euronext":
            path = find_euronext(date)
            if path == None: #coudln't read file
                continue

            df = read_euronext(path)
            if df.empty:
                continue

            ext = path.split('.')[-1]

            if (ext == "csv"):
                insert_euronext_csv(df, db, path)
            else:
                insert_euronext_xlsx(df, db, path)

        if website == "boursorama":

            files = find_boursorama(date)
            if files == None or files == []:
                continue

            stocks = []
            #sorting :D
            files.sort()

            for file in files:
                # for each file detected that month, create and concatenate the stocks DataFrame
                df = read_boursorama(file)
                if df.empty:
                    continue

                #on append les stocks généres
                stocks.append(insert_boursorama(df, db, file, company_id_map))
            

            s = pd.concat(stocks, ignore_index=True)

            # on insert tous les stocks générés pour plus d'efficacité
            try:
                db.df_write(s, 'stocks')
            except Exception as e:
                print("Erreur lors de l'insertion des stocks bourorama:", e)
    
    return




if __name__ == '__main__':
    print("Go Extract Transform and Load")
    pd.set_option('display.max_columns', None)  # usefull for dedugging
    db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
    #db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker
    db._purge_database()
    db._setup_database()
    store_files("2019-01-01", "2022-01-01", "euronext", db)
    store_files("2019-01-01", "2022-01-01", "boursorama", db)
    print("Done Extract Transform and Load")