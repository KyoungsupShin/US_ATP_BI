import pandas as pd
import sys
sys.path.append('../utils')
from utils import *

class master_reset(DB_Utils):
    def __init__(self):
        self.connect_azuredb()
        self.master_df = pd.ExcelFile('../data/master.xlsx',engine='openpyxl')
        self.master_df.sheet_names #equal to DB table name     
        self.reset_master_tables()
        self.push_data()

    def reset_master_tables(self):
        #resete all master dataset
        for sheet in self.master_df.sheet_names:
            sql = 'DELETE FROM {}'.format(sheet)
            self.cursor.execute(sql)
        self.conn.commit()
    
    def push_data(self):
        tablename = self.master_df.sheet_names[0]
        df = pd.ExcelFile('../data/master.xlsx',engine='openpyxl').parse(tablename)
        for i in range(len(df)):
            sql = f'''INSERT INTO {tablename} (MailAddr, Division, RnR) VALUES {tuple(df[['MailAddr','Division','RnR']].values[i])}'''
            self.cursor.execute(sql)
        self.conn.commit()

        tablename = self.master_df.sheet_names[1]
        df = pd.ExcelFile('../data/master.xlsx',engine='openpyxl').parse(tablename)
        for i in range(len(df)):
            sql = f'''INSERT INTO {tablename} (AREA, MailTitle, Domain, ExcelName, UseYN) VALUES {tuple(df[['AREA', 'MailTitle', 'Domain', 'ExcelName', 'UseYN']].values[i])}'''
            self.cursor.execute(sql)
        self.conn.commit()

        tablename = self.master_df.sheet_names[2]
        df = pd.ExcelFile('../data/master.xlsx',engine='openpyxl').parse(tablename)
        for i in range(len(df)):
            sql = f'''INSERT INTO {tablename} (FullStatesName, CodeName, CodeNumber) VALUES {tuple(df[['FullStatesName', 'CodeName', 'CodeNumber']].values[i])}'''
            self.cursor.execute(sql)
        self.conn.commit()

        tablename = self.master_df.sheet_names[3]
        df = pd.ExcelFile('../data/master.xlsx',engine='openpyxl').parse(tablename)
        for i in range(len(df)):
            sql = f'''INSERT INTO {tablename} (ExcelName, Domain, SheetList, TransactionColumn, TargetColumns, UseYN) VALUES 
            {tuple(df[['ExcelName', 'Domain', 'SheetList', 'TransactionColumn','TargetColumns','UseYN']].values[i])}'''
            self.cursor.execute(sql)
        self.conn.commit()

        tablename = self.master_df.sheet_names[4]
        df = pd.ExcelFile('../data/master.xlsx',engine='openpyxl').parse(tablename)
        for i in range(len(df)):
            sql = f'''INSERT INTO {tablename} (WarehouseName, AREA, POD, StreetAddr, CityName, StatesName, PostalCode, FullAddress, UseYN) VALUES 
            {tuple(df[['WarehouseName', 'AREA', 'POD', 'StreetAddr', 'CityName', 'StatesName', 'PostalCode', 'FullAddress', 'UseYN']].values[i])}'''
            self.cursor.execute(sql)
        self.conn.commit()
        self.conn.close()

if __name__ == '__main__':
    mr = master_reset()
