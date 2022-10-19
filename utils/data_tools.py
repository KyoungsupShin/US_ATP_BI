import pandas as pd
import numpy as np
from datetime import datetime
import re
import time
import itertools
import os
import xlrd
import urllib
from datetime import timedelta

class Data_Manipulation():
    def get_cw(self, x): #date -> CW + iso_Week_Number
            return "CW"+"{:02d}".format((x + timedelta(days = 0)).isocalendar()[1])

    def get_yyyy(self, x):
        return str(x[:4])

    def get_yyyymm(self,x):
        return str(x.year) + "{:02d}".format(int(x.month))

    def get_yyyymmdd(self,x):
        return str(x.year) + "{:02d}".format(int(x.month)) + "{:02d}".format(int(x.day))

    def get_dates(self,x):
        return datetime.strftime(x, '%Y-%m-%d')

    def get_tariff_rate(self, x):
        if len(str(x)) >= 1:        
            rate = self.df_tariff['Tariff'].to_numpy()[self.df_tariff['Code'].to_numpy() == str(str(x)[-1])]
            if len(rate) == 0:
                return 0
            else:
                return rate.item()

    def read_date(self, date):
        return xlrd.xldate.xldate_as_datetime(date, 0)

    def yyyymmdd_datetime(self, x):
        if len(x) == 0: #fully string data ex) TBD
            return ''
        if pd.isna(x): # nan value filter
            return '' 
        try:
            if pd.isnull(np.datetime64(x)): #NAT value filter
                return ''    
        except:
            pass
        x = str(x)
        removal_string = ['/', '.', '-', '_', ' ']    
        for rs in removal_string:
            x = x.replace(rs, '')
        if len(x) > 8:
            x = x[:8]
        try:
            return datetime.strftime(datetime.strptime(x, '%m%d%Y'), '%Y-%m-%d')
        except:
            try:
                return datetime.strftime(datetime.strptime(x, '%Y%m%d'), '%Y-%m-%d')
            except:
                return ''
                pass
            pass

    def data_int(self, x):
        if pd.isna(x):
            return 0
        elif (type(x) != int) or (type(x) != float):
            try:
                return int(x)
            except:
                return 0 
                pass
        else:
            return int(x)

    def data_float(self, x):
        try:
            if type(x) == str:
                x = re.findall(r'\d+', x)
                if len(x) == 0:
                    return 0
                else:
                    return float(x)
            elif type(x) == int:
                return float(x)
            elif pd.isna(x):
                return 0
            else: 
                return x
        except:
            return 0
            pass

    def data_text(self, x):
        if pd.isna(x):
            return ''
        else:
            x = str(x)
            x = x.strip()
            x = x.replace("'", "")
            if len(x) > 150:
                return x[:100]
            else:
                return x

class Data_Cleans():
    def clean_data_text(self, func):
        try:
            target_columns_text = self.sheet_info[self.sheet_info['datatype'] == 'TEXT']['targetcolumns'].tolist()
            for col in target_columns_text:
                self.df[col] = self.df[col].apply(lambda x:func(x))
        except Exception as e:
            raise KeyError('TransformDataError: {} file {} column[text] <br>'.format(self.saved_path, col) + str(e))

    def clean_data_int(self, func):
        try:
            target_columns_int = self.sheet_info[self.sheet_info['datatype'] == 'INT']['targetcolumns'].tolist()
            for col in target_columns_int:
                self.df[col] = self.df[col].apply(lambda x:func(x))
        except Exception as e:
            raise KeyError('TransformDataError: {} file {} column[int] <br>'.format(self.saved_path, col) + str(e))

    def clean_data_float(self, func):
        try:
            target_columns_float = self.sheet_info[self.sheet_info['datatype'] == 'FLOAT']['targetcolumns'].tolist()
            for col in target_columns_float:
                self.df[col] = self.df[col].apply(lambda x:func(x))
        except Exception as e:
            raise KeyError('TransformDataError: {} file {} column[float] <br>'.format(self.saved_path, col) + str(e))

    def clean_data_datetime(self, func):
        try:
            target_columns_datetime = self.sheet_info[self.sheet_info['datatype'] == 'DATETIME']['targetcolumns'].tolist()
            for col in target_columns_datetime:
                self.df[col] = self.df[col].apply(lambda x:func(str(x)))        
        except Exception as e:
            raise KeyError('TransformDataError: {} file {} column[datetime] <br>'.format(self.saved_path, col) + str(e))


    def clean_prod_plan_data(self):
        print('[EVENT] GA PRODUCTION PLAN ETL ON PROGRESS. IT NEEDS TO BE UN-PIVOT.')
        self.df = self.df.dropna(how='all', axis='columns')
        self.df = self.df[(self.df['Curr/Futu Prod.'] !='X') & (self.df['MODEL'].notna()) & (self.df['category'] == 'W/H IN (생산)')]
        self.df = self.df.drop(['category', 'Grade','Cell type', 'J.Box', 'Curr/Futu Prod.'], axis = 1)
        self.df = self.df.reset_index(drop = True)
        self.df = self.df.melt(id_vars = ['MODEL', 'Power', 'Item code'])
        self.df = self.df.dropna()
        self.df['variable']= pd.to_datetime(self.df['variable'].apply(self.read_date), errors='coerce')
        self.df = self.df.dropna()
        self.df['variable'] = self.df['variable'].apply(lambda x:self.yyyymmdd_datetime(str(x)))
        self.df = self.df.rename(columns = {'MODEL' : 'ProductName', 'Power' : 'Power_Class', 'Item code' : 'Item_Code',
                                            'variable' : 'Product_Plan_Date', 'value' : 'MW'})
        self.df = self.df[(self.df['ProductName'] !='빈칸') & (self.df['MW'] > 0)]
        
    def attachment_excel_clean(self, path, engine, sheet_name):
        time.sleep(1)
        skiprows = 0
        while True:
            try:
                print('[WARNING] {} / {} EXCEL FILE SKIP ROWS: {}'.format(path, sheet_name, skiprows))
                self.df = pd.read_excel(path
                                    , engine=engine
                                    , sheet_name = sheet_name
                                    , skiprows= skiprows)
                break
            except Exception as e:
                if str(e).split(' ')[0][1:] == 'Worksheet':
                    sheet_name = 0
                else:
                    skiprows = skiprows + 1
                    if skiprows > 5:
                        raise ValueError('ExtractDataError: PLEASE CHECK OUT THE SHEET NAME OR COLUMN SKIP LINE IN THIS EXCEL FILE : {} '.format(path)) 
                    print(e)
                pass
        cols = [col for col in self.df.columns if (col is not None) and (col[:7] != 'Unnamed')] # remove none columns
        self.df = self.df[cols] 
        self.df.columns = [col.strip() for col in self.df.columns] #remove not-stripped columns
        if self.mail_category == 'ORDER_STATUS_REPORT':
            self.df.columns = [col.split('(')[0].rstrip() for col in self.df.columns]
            self.df.rename(columns = {'Task Name' : 'CustomerPO_Num'
                                    , 'Task ID' : 'CR_Num'}, inplace = True)
        self.df.columns = [i.title().replace(' ', '_') if len(i.split(' ')) >= 2 else i for i in self.df.columns]
        self.df = self.df[self.sheet_info.targetcolumns.tolist()]
        return self.df

    def category_replace(self, mail_category_parse):
        if mail_category_parse.lower() == 'inbound_oversea':
            mail_category_parse = 'INBOUND_OVERSEA'
        elif mail_category_parse.lower() == 'inbound_domestic':
            mail_category_parse = 'INBOUND_DOMESTIC'
        elif mail_category_parse.lower() == 'outbound':
            mail_category_parse = 'OUT_BOUND'
        elif mail_category_parse.lower() == 'onhand':
            mail_category_parse = 'ON_HAND'
        elif mail_category_parse.lower() == 'osr':
            mail_category_parse = 'ORDER_STATUS_REPORT'
        elif mail_category_parse.lower() == 'ga_prod_plan':
            mail_category_parse = 'GA_PROD_PLAN'
        else:
            mail_category_parse = 'None'
        return mail_category_parse