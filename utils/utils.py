import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
import win32com
import win32com.client
import re
import time
import itertools
import os
from difflib import SequenceMatcher
import xlrd
from datetime import timedelta

class Global(object):
    root_path = 'C:/Users/qcells/Desktop/ATP/US_ATP_BI'
    driver= '{ODBC Driver 17 for SQL Server}'  
    atp_server = 'qcells-us-atp-db-server.database.windows.net'
    atp_database = 'us-qcells-atp-db'
    atp_username = 'qcells'
    atp_password = '{Asdqwe123!@#}'   
    qsp_server = '11.0.40.144'
    qsp_database = 'master'
    qsp_username = 'qspdev_user'
    qsp_password = '{QsxdRqsp123!#*}'   

class DB_Utils():
    def connect_qspdb(self):
        try:
            self.qsp_conn = pyodbc.connect('DRIVER='+Global.driver
                                    +';SERVER=tcp:'+Global.qsp_server
                                    +';PORT=1433;DATABASE='+Global.qsp_database
                                    +';UID='+Global.qsp_username
                                    +';Pwd='+Global.qsp_password)
            self.qsp_cursor = self.conn.cursor()
        except:
            raise ConnectionError('ConnectionError:AZURE DB CONNECTION FAILED')

    def connect_azuredb(self):
        try:
            self.conn = pyodbc.connect('DRIVER='+Global.driver
                                    +';SERVER=tcp:'+Global.atp_server
                                    +';PORT=1433;DATABASE='+Global.atp_database
                                    +';UID='+Global.atp_username
                                    +';Pwd='+Global.atp_password
                                    +';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
            self.cursor = self.conn.cursor()
        except:
            raise ConnectionError('ConnectionError:AZURE DB CONNECTION FAILED')

    def fetch_data(self, sql):
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchall()
            row = [list(i) for i in row]
            col_names = [item[0] for item in self.cursor.description]
            return pd.DataFrame(row, columns=col_names)
        except:
            raise ConnectionError('ConnectionError:RPA FAILED TO FETCH DATA TO AZURE DB TABLES')

    def insert_dataframe(self, tablename, df, writer = False):
        self.connect_azuredb()
        self.error_check = []
        columns = df.columns.tolist()
        if writer == True:
            columns.append('Writer')
            df['Writer'] = self.writer
        cols_join = ', '.join(str(x) for x in columns)
        print('[EVENT] INSERT INFO: \n', '\t TABLE NAME: ', tablename, '\n', '\t TABLE COLUMNS ARE BELOW', columns)
        for i in range(len(df)):
            try:
                time.sleep(0.01)
                sql = f'''INSERT INTO {tablename} ({cols_join}) VALUES {tuple(df[columns].values[i])}'''                
                self.cursor.execute(sql)
            except Exception as e:                
                print(e)
                print(sql)
                self.error_check.append(str(e))
                pass
        self.conn.commit()
        self.conn.close() 

class QSP_MasterCode(DB_Utils):
    def __init__(self):
        #connect db
        self.isUpadted = False
        self.connect_azuredb()
        self.connect_qspdb()

    def check_isUpdated(self):
        #search data count & compare qty
        sql_qsp = '''
            select count(1) from qsp_master_code
            '''
        sql_atp = '''
            select count(1) from atp_master_code
            '''
        qsp_master_code_qty = self.fetch_data(sql_qsp).values()[0][0]
        atp_master_code_qty = self.fetch_data(sql_qsp).values()[0][0]
        if qsp_master_code_qty - atp_master_code_qty > 0:
            self.isUpadted = True
        else:
            self.isUpadted = False

    def update_master_code(self):
        if self.isUpadted == True:
            sql = 'select * from qsp_master_code'
            master_data = self.fetch_data(self)
            self.insert_dataframe(master_data)
        self.conn.close()
        self.isUpadted = False

class Master_Reset(DB_Utils):
    def check_isadmin(self):
        self.connect_azuredb()
        sql = f'''select count(MailAddr) from ADMIN_INFO 
                WHERE MailAddr = '{self.sender_addr}' '''
        self.checkisadmin = self.fetch_data(sql)
        self.conn.close()

    def reset_master_tables(self):
        self.connect_azuredb()
        try:
            self.master_df = pd.ExcelFile(Global.root_path + '/data/master.xlsx',engine='openpyxl')
            for sheet in self.master_df.sheet_names:
                if (sheet != 'US_CITY_CODE') and (sheet != 'CW_CALENDER') and (sheet != 'ITEM_CODE_MASTER') and (sheet != 'POWER_CLASS_DIST'):
                    sql = 'DELETE FROM {}'.format(sheet)
                    self.cursor.execute(sql)
                    self.conn.commit()
        except Exception as e :
            raise ValueError('MasterResetError:WHILE RESETTING MASTER TABLES, THERE WAS AN ERROR. \n', str(e))

    def push_data(self):
        try:
            print('[EVENT] NEW MASTER DATA IS BEING SAVED !')
            for sheet_name in self.master_df.sheet_names:
                if (sheet_name != 'US_CITY_CODE') and (sheet_name != 'CW_CALENDER') and (sheet_name != 'ITEM_CODE_MASTER') and (sheet_name != 'POWER_CLASS_DIST'):
                    df = pd.ExcelFile(self.file_path,engine='openpyxl').parse(sheet_name)
                    # df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                    df = df.dropna(how='all', axis='columns')
                    df.dropna(inplace = True, how = 'all')
                    df.fillna('', inplace = True)
                    if sheet_name == 'POWER_CLASS_DIST':
                        df = self.unpivot_powerclassdist(df)
                    if sheet_name == 'CW_CALENDER':
                        df['Friday'] = df['Friday'].astype('str')
                    if sheet_name == 'TARIFF_INFO':
                        df = self.unpivot_taiffinfo(df)
                    self.insert_dataframe(sheet_name, df)
            print('\n')
        except Exception as e:
            raise ValueError('MasterResetError:WHILE PUSHING DATA TO MASTER TABLES, THERE WAS AN ERROR. \n', str(e))

    def unpivot_powerclassdist(self, df):
        power_df = pd.melt(df, id_vars=['FactoryCd','Product_Name', 'SAP_UNIQUE_CODE', 'Power_Class', 'Segment'], value_vars=df.columns[5:])
        power_df = power_df.rename(columns = {'variable' : 'YYYYMM', 'value' : 'Distribution'})
        power_df.YYYYMM = power_df.YYYYMM.astype('str')
        return power_df
        
    def unpivot_taiffinfo(self, df):
        df = df.melt(id_vars = ['Year', 'Month', 'YYYYMM'], value_vars=df.columns[3:]).rename(columns = {'variable' : 'FactoryCd', 'value' : 'TariffCd'})
        return df

    def master_reset_main(self, sender_addr, file_path = Global.root_path+'/data/master.xlsx'): 
        self.file_path = file_path
        self.reset_master_tables()
        self.push_data()

class ETL_Utils(DB_Utils):
    def meta_sheet_info(self, excel_name):
        self.connect_azuredb()
        self.excel_name= excel_name
        sql = f'''select sheetlist, targetcolumns, datatype from SHEET_INFO
                where MailCategory = '{excel_name}' and BackUpYN = 'Y' '''
        self.sheet_info = self.fetch_data(sql)
        self.target_sheet_list = list(set(self.sheet_info['sheetlist'].tolist()))
        self.conn.close()

    def order_status_column_filter(self):
        self.df.columns = [col.split('(')[0].rstrip() for col in self.df.columns]
        self.df.rename(columns = {'Task Name' : 'CustomerPO_Num'
                            , 'Task ID' : 'CR_Num'}, inplace = True)

    def read_rpa_excel_file(self, path, engine, sheet_name):
        time.sleep(1)
        skiprows = 0
        if self.excel_name == 'ORDER_STATUS_REPORT':
            skiprows = 2
        while True:
            try:
                print('[WARNING] {} / {} EXCEL FILE SKIP ROWS: {}'.format(path, sheet_name, skiprows))
                self.df = pd.read_excel(path
                                    , engine=engine
                                    , sheet_name = sheet_name
                                    , skiprows= skiprows)
                self.df = self.df.loc[:, ~self.df.columns.str.contains('^Unnamed')]
                break
            except Exception as e:
                skiprows = skiprows + 1
                if skiprows > 5:
                    raise ValueError('PLEASE CHECK OUT THE COLUMN LINE IN {} EXCEL FILE'.format(path)) 
                print(e)
                pass
        self.df = self.df.dropna(how = 'all')
        self.df.columns = [col.strip() for col in self.df.columns]

        if self.excel_name == 'ORDER_STATUS_REPORT':
            self.order_status_column_filter()
        self.df = self.df[self.sheet_info.targetcolumns.tolist()]
        return self.df

    def read_excel(self, path = Global.root_path):
        try:
            print('[EVENT] EXCEL FILE IS READING. \n PATH: {}'.format(path))
            if path.split('.')[-1].lower() == 'xlsb':
                self.df = self.read_rpa_excel_file(path,engine='pyxlsb', 
                                        sheet_name=self.target_sheet_list[0])
            elif path.split('.')[-1].lower() == 'csv':
                self.df = pd.read_csv(path, encoding = 'cp949')
            else:
                self.df = self.read_rpa_excel_file(path,engine='openpyxl', 
                                        sheet_name=self.target_sheet_list[0])        
            if 'Updated_date' in self.df.columns:
                self.check_date_sql = self.df['Updated_Date'].unique().values[0][0]
                print('[EVENT] EXCEL FILE HAS UPDATED_DATE COLUMN. UPDATE_DATE: {}'.format(self.check_date_sql))
            else:
                self.check_date_sql = datetime.now().strftime('%Y-%m-%d')
                self.df['Updated_Date'] = self.check_date_sql
                print('[EVENT] EXCEL FILE DOES NOT HAVE UPDATED_DATE COLUMN. RPA WILL CREATE. UPDATE_DATE: {}'.format(self.check_date_sql))
        except Exception as e:
            raise KeyError('ExtractDataError:' + str(e))

    def Check_file_is_updated(self):        
        self.connect_azuredb()
        sql = f'''
                SELECT COUNT(1) FROM {self.excel_name}
                WHERE Updated_Date = '{self.check_date_sql}' '''
        sql2 = f'''
                DELETE FROM {self.excel_name}
                WHERE Updated_Date = '{self.check_date_sql}' '''

        if int(self.fetch_data(sql).values[0][0]) > 1:
            print('[EVENT] {} DATASET HAS ALREADY SAVED TODAY. RPA IS INITIALIZING NOW'.format(self.excel_name))
            self.cursor.execute(sql2)
            self.conn.commit() 
        else:
            pass
        self.conn.close()

    def ETL_error_check(self):
        self.connect_azuredb()
        if len(self.error_check) > 1:
            print('[WARNING] DURING ETL ERROR OCCURRED. WARNING MAIL SENDING')
            sql = f'''select MailAddr from ADMIN_INFO WHERE RnR = 'Dev' '''
            checkisadmin = self.fetch_data(sql)
            raise KeyError('ExtractDataError: ERROR INFORMING DURING ETL PROCESSING')
        self.conn.close()

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
        if len(x) == 6:
            x = x + '01'
        else:
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
            x = x.replace("'", "")
            if len(x) > 150:
                return x[:100]
            else:
                return x

    def data_null_check(self, *args):
        self.null_check = False
        if len(args) > 0: 
            isNull_check_result = [i for i in args if len(str(i)) == 0]
            if len(isNull_check_result) > 0:
                self.null_check = True
                raise ValueError('TransformDataError: THERE IS NULL ROWS. NEED TO CHECK.')
            else:
                pass

    def read_date(self, date):
        return xlrd.xldate.xldate_as_datetime(date, 0)

    def data_prod_plan_clean(self):
        self.excel_name = 'GA_PROD_PLAN'
        self.df = pd.read_excel(self.saved_path
                            , engine='pyxlsb'
                            , sheet_name = 'Revised_Detail PSI (item code)'
                            , skiprows= 4)
        self.df = self.df.dropna(how='all', axis='columns')
        self.df = self.df[self.df['Curr/Futu Prod.'] !='X']
        self.df = self.df[self.df['MODEL'].notna()]
        self.df = self.df[self.df['category'] == 'W/H IN (생산)']
        self.df = self.df.drop(['category', 'Grade','Cell type', 'J.Box', 'Curr/Futu Prod.'], axis = 1)
        self.df = self.df.reset_index(drop = True)
        self.df = self.df.melt(id_vars = ['MODEL', 'Power', 'Item code'])
        self.df['variable']= pd.to_datetime(self.df['variable'].apply(self.read_date), errors='coerce')
        self.df = self.df.dropna()
        self.df['variable'] = self.df['variable'].apply(lambda x:self.yyyymmdd_datetime(str(x)))
        self.df = self.df.rename(columns = {'MODEL' : 'ProductName',
                            'Power' : 'Power_Class',
                            'Item code' : 'Item_Code',
                            'variable' : 'Product_Plan_Date', 
                            'value' : 'MW'})
    
    def clean_data_text(self, func):
        try:
            target_columns_text = self.sheet_info[self.sheet_info['datatype'] == 'TEXT']['targetcolumns'].tolist()
            for col in target_columns_text:
                self.df[col] = self.df[col].apply(lambda x:func(x))
        except Exception as e:
            raise KeyError('TransformDataError: {} file {} column[text] \n'.format(self.saved_path, col) + str(e))

    def clean_data_int(self, func):
        try:
            target_columns_int = self.sheet_info[self.sheet_info['datatype'] == 'INT']['targetcolumns'].tolist()
            for col in target_columns_int:
                self.df[col] = self.df[col].apply(lambda x:func(x))
        except Exception as e:
            raise KeyError('TransformDataError: {} file {} column[int] \n'.format(self.saved_path, col) + str(e))

    def clean_data_float(self, func):
        try:
            target_columns_float = self.sheet_info[self.sheet_info['datatype'] == 'FLOAT']['targetcolumns'].tolist()
            for col in target_columns_float:
                self.df[col] = self.df[col].apply(lambda x:func(x))
        except Exception as e:
            raise KeyError('TransformDataError: {} file {} column[float] \n'.format(self.saved_path, col) + str(e))

    def clean_data_datetime(self, func):
        self.connect_azuredb()
        try:
            target_columns_datetime = self.sheet_info[self.sheet_info['datatype'] == 'DATETIME']['targetcolumns'].tolist()
            for col in target_columns_datetime:
                self.df[col] = self.df[col].apply(lambda x:func(str(x)))        
        except Exception as e:
            raise KeyError('TransformDataError: {} file {} column[datetime] \n'.format(self.saved_path, col) + str(e))

        self.Check_file_is_updated()
        self.insert_dataframe(self.excel_name, self.df, writer = True)

class ETL_Pipelines(ETL_Utils, DB_Utils):
    def __init__(self, saved_path='', mail_category='', check_date = datetime.now().strftime('%Y-%m-%d'), writer = 'SYSTEM', manual = False):
        self.saved_path = saved_path
        self.mail_category = mail_category
        self.check_date_sql = check_date # => dataframe의 updated_date를 참조
        self.writer = writer
        if manual == False:
            self.main()

    def ETL_Basic_Process(self):
        self.read_excel(path = self.saved_path)
        self.data_null_check()
        self.clean_data_text(self.data_text)
        self.clean_data_int(self.data_int)
        self.clean_data_int(int)
        self.clean_data_float(self.data_float)
        self.clean_data_float(float)
        self.clean_data_datetime(self.yyyymmdd_datetime)

    def ON_HAND_ETL(self):
        self.meta_sheet_info(excel_name = 'ON_HAND')
        self.ETL_Basic_Process()
        
    def ORDER_STATUS_REPORT_ETL(self):
        self.check_isnull_columns = ['ETD', 'ETA']
        self.meta_sheet_info(excel_name = 'ORDER_STATUS_REPORT')
        self.ETL_Basic_Process()
        
    def OUT_bound_ETL(self):
        self.meta_sheet_info(excel_name = 'OUT_BOUND')
        self.ETL_Basic_Process()
    
    def IN_BOUND_OVERSEA_ETL(self):
        self.meta_sheet_info(excel_name = 'INBOUND_OVERSEA')
        self.ETL_Basic_Process()

    def IN_BOUND_DOMESTIC_ETL(self):
        self.meta_sheet_info(excel_name = 'INBOUND_DOMESTIC')
        self.ETL_Basic_Process()

    def GA_PROD_PLAN_ETL(self):
        # self.read_excel(path = self.saved_path)        
        self.data_prod_plan_clean()
        self.Check_file_is_updated()
        self.insert_dataframe('GA_PROD_PLAN', self.df, writer = True)

    def main(self):
        print('[EVENT] DATA ETL PIPELINE JUST BEGAN : {} !'.format(self.mail_category))
        if self.mail_category == 'INBOUND_OVERSEA':
            self.IN_BOUND_OVERSEA_ETL()
        if self.mail_category == 'INBOUND_DOMESTIC':
            self.IN_BOUND_DOMESTIC_ETL()
        if self.mail_category == 'OUT_BOUND':
            self.OUT_bound_ETL()
        if self.mail_category == 'ON_HAND':
            self.ON_HAND_ETL()
        if self.mail_category == 'ORDER_STATUS_REPORT':
            self.ORDER_STATUS_REPORT_ETL()
        if self.mail_category == 'GA_PROD_PLAN':
            self.GA_PROD_PLAN_ETL()
        self.ETL_error_check()

class Email_Utils(Master_Reset, ETL_Utils):
    def __init__(self, mail_receivers =  "digital_scm@us.q-cells.com"):
        super().__init__()
        self.mail_receivers = mail_receivers
        self.outlook = win32com.client.Dispatch("Outlook.Application")
        self.Rxoutlook = self.outlook.GetNamespace("MAPI")
        self.recip = self.Rxoutlook.CreateRecipient(self.mail_receivers)
        self.similarity_threshold = 0.5

    def send_email(self, mail_title, content_title, content_body, destination, attachment_path=''):    
        try:
            with open("../utils/template.html", "r", encoding='utf-8') as f:
                text= f.read()
            text = text.replace('RPA-TITLE' , content_title)        
            text = text.replace('RPA-CONTENTS', content_body)        
            Txoutlook = self.outlook.CreateItem(0)
            Txoutlook.To = destination
            Txoutlook.Subject = mail_title
            Txoutlook.HTMLBody = f"""{text}"""
            if attachment_path:
                Txoutlook.Attachments.Add(attachment_path)
            Txoutlook.Send()
            print('[EVENT] SENT MAIL TO {}'.format(destination))
        except Exception as e:
            raise KeyError('EmailError: THERE WAS AN ERROR DURING SENDING MAIL. ERROR MESSGAE IS: ' + str(e))

    def sender_mailaddr_extract(self, i):
        if i.SenderEmailType=='EX':
            if i.Sender.GetExchangeUser() != None:
                return i.Sender.GetExchangeUser().PrimarySmtpAddress
            else:
                return i.Sender.GetExchangeDistributionList().PrimarySmtpAddress
        else:
            return i.SenderEmailAddress

    def access_mailbox(self):
        try:
            self.inbox = self.Rxoutlook.GetSharedDefaultFolder(self.recip, 6)
        except:
            print("[WARNING] OUTLOOK APP WAS RESTARTED. TRYING TO RE-CONNECT")
            self.outlook = win32com.client.Dispatch("Outlook.Application")
            self.Rxoutlook = self.outlook.GetNamespace("MAPI")    
            self.recip = self.Rxoutlook.CreateRecipient(self.mail_receivers)  
            self.inbox = self.Rxoutlook.GetSharedDefaultFolder(self.recip, 6)
            pass
        print('[ITERATION] {} TOTAL E-MAIL FILED: {}'.format(datetime.now().strftime('%Y-%m-%d'),len(self.inbox.items)))
    
    def mail_target_extension_filter(self, download_filetype):
        atts = []
        for filetype in range(len(download_filetype)):
            atts.append([att for att in self.i.Attachments if download_filetype[filetype] in att.FileName.split('.')[-1].lower()]) #specific extension filtering
        self.atts = list(itertools.chain(*atts))                
        self.sender_addr = self.sender_mailaddr_extract(self.i)

    def category_replace(self, mail_category_parse):
        #MAIL LIST SHEET에서 참조함.
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
            # raise ValueError('ValueError:THERE IS NO CATEGORY IN THE LIST, ex) inbound -> IN_BOUND')

    def extract_request_update_mail_info(self):
        try:
            title_parsed = self.i.subject.lower().split('/')
            mail_category_parse = re.sub(r'[^a-zA-Z]', '', title_parsed[1])
            self.mail_category_parse = self.category_replace(mail_category_parse)
            self.Req_date = datetime.now().strftime('%Y-%m-%d')
            if len(title_parsed) >= 3:
                if len(title_parsed[2]) == 8:
                    self.Req_date = self.yyyymmdd_datetime(title_parsed[2])                
        except:
            raise ValueError('EmailError:THERE IS SOMETHING WRONG IN MAIL TITLE FOR PARSING CATEGORIES.')

    def invalidate_whitelist(self, mail_title, attch_name, mail_domain):
        self.connect_azuredb()
        sql = 'select Mailtitle, Domain, ExcelName, MailCategory from MAIL_LIST ml'
        whitelist_dict = self.fetch_data(sql)
        whitelist_dict = whitelist_dict.to_dict('records')
        mail_domain = mail_domain.split('@')[-1]
        similarity = 0
        similarity_attach = 0
        isdomainsame = False
        mail_category_parse = 'None'
        for idx, whitelist in enumerate(whitelist_dict):
            if similarity <= SequenceMatcher(None, whitelist['Mailtitle'], mail_title).ratio():
                similarity = SequenceMatcher(None, whitelist['Mailtitle'], mail_title).ratio()
                max_ssim_title = whitelist['Mailtitle']
                max_ssim_domian = whitelist_dict[idx]['Domain']     
                     
                for idx, whitelist in enumerate(whitelist_dict):
                    if similarity_attach <= SequenceMatcher(None, whitelist['ExcelName'], attch_name).ratio():
                        similarity_attach = SequenceMatcher(None, whitelist['ExcelName'], attch_name).ratio()
                        max_ssim_excel = whitelist['ExcelName']
                        mail_category_parse = whitelist['MailCategory']
                        self.mail_category_parse= self.category_replace(mail_category_parse)                
                if (mail_domain == max_ssim_domian) & (similarity > self.similarity_threshold) & (similarity_attach > self.similarity_threshold):
                    isdomainsame = True
                    break
                else:
                    isdomainsame = False
        return similarity, isdomainsame, self.mail_category_parse

    def save_attachments(self, att, i, save_date = datetime.now().strftime('%Y-%m-%d')):
        print('[EVENT] SAVED ATTACHMENTS: {}'.format(att.FileName))
        saved_dir = Global.root_path + '/data/' + save_date
        saved_path = saved_dir + '/' + att.FileName
        os.makedirs(saved_dir, exist_ok = True)
        att.SaveAsFile(saved_path)
        ETL_Pipelines(saved_path, self.mail_category_parse, save_date, writer = self.sender_addr.split('@')[0])

    def save_attachments_dummy(self, att, i, save_date = datetime.now().strftime('%Y-%m-%d')):
        print('[EVENT] SAVED ATTACHMENTS: {}'.format(att.FileName))
        saved_dir = Global.root_path + '/data/' + save_date
        saved_path = saved_dir + '/' + att.FileName
        os.makedirs(saved_dir, exist_ok = True)
        att.SaveAsFile(saved_path)
        # saved_path = Global.root_path + '/data/dummy/{}.xlsx'.format(self.mail_category_parse)
        # ETL_Pipelines(saved_path, self.mail_category_parse, save_date, writer = self.sender_addr.split('@')[0])

    def write_logs(self, FileName, Result, Received_time, ExcelType):
        print('[EVENT] SAVED ATTACHMENTS LOG WROTTEN IN RPA_DOWNLOAD_LOGS')
        self.connect_azuredb()
        sql = f'''INSERT INTO RPA_DOWNLOAD_LOGS (ExcelName, Result, MailReceivedDate, ExcelType)
                VALUES('{FileName}', '{Result}', '{Received_time}', '{ExcelType}');'''
        self.cursor.execute(sql)
        self.conn.commit()
        self.conn.close()

    def write_error_logs(self, error_name, error_type, Mail_Sender):
        print('[EVENT] SAVED ATTACHMENTS LOG WROTTEN IN ERROR_LOGS')
        self.connect_azuredb()
        sql = f'''INSERT INTO ERORR_LOGS (Error_Name, RPA_Type, Mail_Sender)
                VALUES('{error_name}', '{error_type}', '{Mail_Sender}');'''
        print(sql)
        self.cursor.execute(sql)
        self.conn.commit()
        self.conn.close()
