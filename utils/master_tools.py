import glob
import os
import sys
from datetime import datetime
sys.path.append('../utils')
# from utils import *
from db_tools import * 
import threading
import time

class Master_Reset(DB_Utils): #Overall Process: get master excel file -> read -> preprocessing -> initialize master table -> insert new data
    def reset_master_tables(self): # Initialize master data.
        self.bypass_sheets = ['US_CITY_CODE', 'POWER_CLASS_DIST', 'CW_CALENDER'] # Not-need updated master dataset(excel sheet)
        try:
            print(Global.root_path)
            self.master_df = pd.ExcelFile(Global.root_path + '/data/master.xlsx',engine='openpyxl') #Read new master excel file
            for self.sheet in self.master_df.sheet_names: 
                if self.sheet not in self.bypass_sheets:
                    self.sql_execute_commit('DELETE FROM {}'.format(self.sheet))                    
                    self.push_new_master_data() # new master data push
                else:
                    pass
        except Exception as e :
            raise ValueError('MasterResetError: WHILE INITIALIZE MASTER TABLES IN ATP DB, THERE WAS AN ERROR. <br>', str(e))

    def push_new_master_data(self): #save new master dataset
        try:
            print('[EVENT] NEW MASTER DATA <<{}>> IS BEING SAVED !'.format(self.sheet))
            df = self.master_df.parse(self.sheet) # read single sheet of master excel file
            df = self.manipulation_master_data(df) # pre-processing data
            self.insert_dataframe_to_db(self.sheet, df) # insert data to ATP DB
            print('\n')
        except Exception as e:
            raise ValueError('MasterResetError: WHILE PUSHING DATA TO MASTER TABLES, THERE WAS AN ERROR. <br>', str(e))

    def manipulation_master_data(self, df): #pre processing 
        df.dropna(how = 'all', axis = 'columns', inplace = True) #remove all null columns, rows 
        df.dropna(how = 'all', inplace = True)
        df.fillna('', inplace = True)
        if self.sheet == 'POWER_CLASS_DIST':
            df = self.unpivot_powerclassdist(df)
        if self.sheet == 'CW_CALENDER':
            df['Friday'] = df['Friday'].astype('str')
        if self.sheet == 'TARIFF_INFO':
            df = df = df.melt(id_vars = ['Year', 'Month', 'YYYYMM'], value_vars=df.columns[3:]).rename(columns = {'variable' : 'FactoryCd', 
                                                                                                                    'value' : 'TariffCd'})
        return df

    def unpivot_powerclassdist(self, df):
        power_df = pd.melt(df, id_vars=['FactoryCd','Product_Name', 'DummyID', 'SAP_UNIQUE_CODE', 'Power_Class', 'Segment'], value_vars=df.columns[6:])
        power_df = power_df.rename(columns = {'variable' : 'YYYYMM', 'value' : 'Distribution'})
        power_df.YYYYMM = power_df.YYYYMM.astype('str')
        return power_df
        
    def master_reset_main(self, sender_addr, file_path = Global.root_path+'/data/master.xlsx'): 
        self.file_path = file_path
        self.reset_master_tables()

if __name__ == '__main__':
    mr = Master_Reset()
    mr.master_reset_main(sender_addr = 'dany.shin@hanwha.com')