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
     date = date.isoformat()
     pattern = re.compile(rf"{date} (\d{2}:\d{2}):\d{2}\.\d+")
     path = os.path.join(HOME, "boursorama", date.year, f"compA {date.strftime("%Y-%m-%d")}")
     if not os.path.exists(path):
         print(f"Fichier Boursorama {path} introuvable.")
         return None
     return path
 
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
         print(df.iloc[np.random.randint(0, len(df))])
         headers = df.columns.tolist()
         return df, headers
     except Exception as e:
         print(f"Erreur lors de la lecture de {path}: {e}")
         return None, None
 
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
 
         if website == "boursorama":
             path = find_boursorama(date)
             if path == None:
                 continue
 
         try:
             # Récupération des marchés
             markets = pd.DataFrame()
             markets['name'] = df[['Name']]
             markets['alias'] = None #df['Symbol'].str.lower()
             markets['boursorama'] = None
             markets['sws'] = None
             markets['euronext'] = df['Market']
               
             # Récupération des entreprises
             #companies = df[['Name', 'ISIN', 'Symbol', 'Market']].drop_duplicates()
             #companies['mid'] = companies['Market'].map(lambda x: db.market_id.get(x.lower(), 100))  # 100 = International si inconnu
             #companies['boursorama'] = None
             #companies['euronext'] = companies['Market']
             #companies['pea'] = False
             #companies[['sector1', 'sector2', 'sector3']] = None
             
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
 
             try:
                 db.df_write(markets, 'markets', commit=True)
             except Exception as e:
                 print("Table market already exsits.")
                 
             #db.df_write(companies, 'companies', if_exists='append')
             #db.df_write(stocks, 'stocks', if_exists='append')
             #db.df_write(daystocks, 'daystocks', if_exists='append')
 
             print(f"Fichier {path} indexé avec succès.")
 
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
     print("Done Extract Transform and Load")