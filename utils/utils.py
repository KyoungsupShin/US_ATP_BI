import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
import win32com
import win32com.client
import re
import threading
import time
import itertools
from difflib import SequenceMatcher
import os
from logger import Report_Log

class Global(object):
    root_path = 'C:/Users/qcells/Desktop/ATP/US_ATP_BI'

class DB_Utils():
    def connect_sapdb(self):
        pass

    def connect_azuredb(self):
        server = 'qcells-us-atp-db-server.database.windows.net'
        database = 'us-qcells-atp-db'
        username = 'qcells'
        password = '{Asdqwe123!@#}'   
        driver= '{ODBC Driver 17 for SQL Server}'  
        self.conn = pyodbc.connect('DRIVER='+driver
                                +';SERVER=tcp:'+server
                                +';PORT=1433;DATABASE='+database
                                +';UID='+username
                                +';Pwd='+password
                                +';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
        self.cursor = self.conn.cursor()
    
    def fetch_data(self, sql):
        self.cursor.execute(sql)
        row = self.cursor.fetchall()
        row = [list(i) for i in row]
        col_names = [item[0] for item in self.cursor.description]
        return pd.DataFrame(row, columns=col_names)

    def insert_dataframe(self, tablename, df):
        cols = ', '.join(str(x) for x in df.columns)
        print(tablename, cols)
        for i in range(len(df)):
            time.sleep(0.02)
            sql = f'''INSERT INTO {tablename} ({cols}) VALUES {tuple(df[df.columns].values[i])}'''
            self.cursor.execute(sql)
        self.conn.commit() 

class ETL_Utils(DB_Utils):
    def meta_sheet(self, excel_name):
        sql = f'''select sheet_list, primary_key, target_columns from meta_sheet_info
                where file_name = {excel_name}'''
        self.sheet_info = self.fetch_data(sql)

    def clean_data(self):
        pk = self.sheet_info[self.sheet_info == sheet]['primary_key'].values[0]
        target_cols = self.sheet_info[self.sheet_info == sheet]['target_columns'].tolist()
        
        df = df.dropna(subset = pk).reset_index(drop = True)
        df = df[target_cols]
        return df

    def read_excel(self, path):
        try:
            if path[-4:] == 'xlsb':
                df_obj = pd.ExcelFile(path,engine='pyxlsb')
            else:
                df_obj = pd.ExcelFile(path,engine='openpyxl')

            self.df_list = []
            self.meta_sheet(path)

            for sheet in self.sheet_info['sheet_list'].tolist():
                df = df_obj.parse(sheet_name=sheet)
                df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                df = self.clean_data(df)
                self.df_list.append(df)
        except Exception as e:
            print(e)
            self.send_email(path, e)
            pass

class SAP_Item_Code(DB_Utils):
    def __init__(self):
        self.connect_sapdb()
        self.connect_azuredb()
        self.fetch_itemcode_from_sap()
        self.insert_itemcode_to_azuredb()
    
    def fetch_itemcode_from_sap(self):
        #read data from sap db
        self.conn_sap.close()
        pass

    def insert_itemcode_to_azuredb(self):
        #insert data to azure db
        self.conn.close()
        pass
       
class Master_Reset(DB_Utils):
    def check_isadmin(self, sender_addr):
        sql = f'''select count(MailAddr) from ADMIN_INFO 
                WHERE MailAddr = '{sender_addr}' '''
        self.checkisadmin = self.fetch_data(sql)

    def reset_master_tables(self):
        for sheet in self.master_df.sheet_names:
            sql = 'DELETE FROM {}'.format(sheet)
            self.cursor.execute(sql)
        self.conn.commit()

    def push_data(self):
        print('[EVENT] NEW MASTER DATA IS BEING SAVED !')
        for sheet_name in self.master_df.sheet_names:
            df = pd.ExcelFile(Global.root_path + '/data/master.xlsx',engine='openpyxl').parse(sheet_name)
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            self.insert_dataframe(sheet_name, df)
        print('\n')
        
    def master_reset_main(self, sender_addr): 
        self.connect_azuredb()
        self.check_isadmin(sender_addr)
        
        if self.checkisadmin.values[0][0] == 1:
            print('[EVENT] Master Reset request authorized !')
            self.master_df = pd.ExcelFile(Global.root_path + '/data/master.xlsx',engine='openpyxl')
            self.master_df.sheet_names #equal to DB table name     
            self.reset_master_tables()
            self.push_data()
            self.conn.close()
            return True
        else:
            print('[WARNING] send e-mail that rejected due to low-authorization')
            self.conn.close()
            return False
        

        
class Email_Utils(Master_Reset):
    def __init__(self, mail_receivers):
        super().__init__()
        self.mail_receivers = mail_receivers
        self.outlook = win32com.client.Dispatch("Outlook.Application")
        self.Rxoutlook = self.outlook.GetNamespace("MAPI")
        self.recip = self.Rxoutlook.CreateRecipient(self.mail_receivers)
        self.similarity_threshold = 0.5
    
    def send_email(self, subject, body, destination, master = False):    
        Txoutlook = self.outlook.CreateItem(0)
        Txoutlook.To = destination
        Txoutlook.Subject = subject
        Txoutlook.HTMLBody = f"""{body}"""
        if master == True:
            Txoutlook.Attachments.Add(Global.root_path + '/data/master.xlsx')
        Txoutlook.Send()    
        print('[EVENT] SENT MAIL TO {}'.format(destination))

    def sender_mailaddr_extract(self, i):
        if i.SenderEmailType=='EX':
            if i.Sender.GetExchangeUser() != None:
                return i.Sender.GetExchangeUser().PrimarySmtpAddress
            else:
                return i.Sender.GetExchangeDistributionList().PrimarySmtpAddress
        else:
            return i.SenderEmailAddress
    
    def invalidate_whitelist(self, mail_title, mail_domain):
        self.connect_azuredb()
        sql = 'select Mailtitle, Domain from MAIL_LIST ml'
        whitelist_dict = self.fetch_data(sql)
        whitelist_dict = whitelist_dict.to_dict('records')
        mail_domain = mail_domain.split('@')[-1]
        similarity = 0
        
        for idx, whitelist in enumerate(whitelist_dict):
            if similarity <= SequenceMatcher(None, whitelist['Mailtitle'], mail_title).ratio():
                similarity = SequenceMatcher(None, whitelist['Mailtitle'], mail_title).ratio()
                max_ssim_title = whitelist['Mailtitle']
                max_ssim_domian = whitelist_dict[idx]['Domain']                
                if (mail_domain == max_ssim_domian) & (similarity > self.similarity_threshold):
                    isdomainsame = True
                    break
                else:
                    isdomainsame = False
        return similarity, isdomainsame
    
    def write_logs(self, FileName, Result):
        self.connect_azuredb()
        sql = f'''INSERT INTO RPA_DOWNLOAD_LOGS (ExcelName, Result)
                VALUES('{FileName}', '{Result}');'''
        self.cursor.execute(sql)
        self.conn.commit()
        self.conn.close()

    def recevie_email(self, check_sd, download_filetype, saveYN):
        try:
            inbox = self.Rxoutlook.GetSharedDefaultFolder(self.recip, 6)
        except:
            print("[WARNING] OUTLOOK APP WAS RESTARTED. TRYING TO RE-CONNECT")
            self.outlook = win32com.client.Dispatch("Outlook.Application")
            self.Rxoutlook = self.outlook.GetNamespace("MAPI")    
            self.recip = self.Rxoutlook.CreateRecipient(self.mail_receivers)  
            inbox = self.Rxoutlook.GetSharedDefaultFolder(self.recip, 6)
            pass

        print('[ITERATION] TOTAL E-MAIL FILED: {}'.format(len(inbox.items)))
        for i in inbox.items: #inbox mail iteration
            atts = []
            try:
                if datetime.strptime(i.SentOn.strftime('%Y-%m-%d'), '%Y-%m-%d') >= datetime.strptime(check_sd, '%Y-%m-%d'): #YYYYMMDD previous mail filtering out
                    for filetype in range(len(download_filetype)):
                        atts.append([att for att in i.Attachments if download_filetype[filetype] in att.FileName.split('.')[-1].lower()]) #specific extension filtering
                    atts = list(itertools.chain(*atts))
                    sender_addr = self.sender_mailaddr_extract(i)

                    if i.subject.lower() == 'request master':
                        print('[EVENT] RECEIVED REQUEST MASTER XLSX ATTACHMENTS')
                        self.send_email('[RPA] MASTER FILE SHARING', 'remaster file here!'
                                        ,destination = self.sender_mailaddr_extract(i) 
                                        ,master = True)
                        i.Delete()
                        continue
                            
                    elif i.subject.lower() == 'reset master':
                        if len(atts) > 0:
                            print('[EVENT] RECEIVED RESET MASTER XLSX ATTACHMENTS')
                            for att in atts:
                                if att.FileName == 'master.xlsx':
                                    os.makedirs(Global.root_path + '/data/MASTSER_HIST', exist_ok = True)
                                    att.SaveAsFile(Global.root_path + '/data/MASTSER_HIST/' + i.SentOn.strftime('%Y%m%d%H%M%S') + '_' + att.FileName) #saving Backup file
                                    att.SaveAsFile(Global.root_path + '/data/' + att.FileName) # saving Master file
                                    reset_result = self.master_reset_main(sender_addr)
                                    if reset_result == True:
                                        self.send_email('[RPA] MASTER FILE RESET RESULT', 'This is the file used!'
                                                        ,destination = self.sender_mailaddr_extract(i)
                                                        ,master = True)
                                    else:
                                        self.send_email('[RPA] MASTER FILE RESET RESULT', 'You are not authorized to reset master!'
                                                        ,destination = self.sender_mailaddr_extract(i)
                                                        ,master = False)

                            self.write_logs('MASTER', 'PASS')
                            i.Delete()  
                            continue
                    else:
                        if len(atts) > 0: #attachment over 1
                            similarity, isdomainsame = self.invalidate_whitelist(i.subject, sender_addr)
                            if (similarity > self.similarity_threshold) & (isdomainsame == True): #title ssim over 0.9, domain filtering
                                print('\n' + '=' * 10, i.SentOn.strftime('%Y-%m-%d'), '=' * 10)
                                print(i.subject) # mail title
                                print(i.Sender, sender_addr, i.CC) #mail sender
                                for att in atts:
                                    print(att.FileName) #attachment name    
                                    if saveYN == True:
                                        print('[EVENT] SAVED ATTACHMENTS')
                                        os.makedirs(Global.root_path + '/data/' + i.SentOn.strftime('%Y-%m-%d'), exist_ok = True)
                                        att.SaveAsFile(Global.root_path + '/data/' +i.SentOn.strftime('%Y-%m-%d') + '/' + att.FileName)
                                    self.write_logs(att.FileName, 'PASS')
                                i.Move([i for i in self.Rxoutlook.Folders if str(i) == 'ATP_ATTACHMENTS'][0])
                                print('\n')  
                                continue
                            
                            elif (i.subject.split(' ')[0] == '[ManualUpdate]'):
                                self.check_isadmin(sender_addr)
                                if self.checkisadmin.values[0][0] == 1:    
                                    print('\n' + 'Manual Update ' + '=' * 10, i.SentOn.strftime('%Y-%m-%d'), '=' * 10)
                                    print(i.subject) # mail title
                                    print(i.Sender, sender_addr, i.CC) #mail sender
                                    for att in atts:
                                        print(att.FileName) #attachment name    
                                        print('[EVENT] SAVED ATTACHMENTS')
                                        os.makedirs(Global.root_path + '/data/' + i.SentOn.strftime('%Y-%m-%d'), exist_ok = True)
                                        att.SaveAsFile(Global.root_path + '/data/' +i.SentOn.strftime('%Y-%m-%d') + '/' + att.FileName)
                                        self.write_logs(att.FileName, 'PASS')
                                    i.Move([i for i in self.Rxoutlook.Folders if str(i) == 'ATP_ATTACHMENTS'][0])
                                    print('\n')  
                                    continue

                else:
                    i.Delete()
            except Exception as e:
                print(e)
                i.Delete()
                pass
            
        print('[ITERATION] INBOX CHECKING JUST DONE')
            

    if __name__ == '__main__':
        db = DB_Utils()
        db.connect_azuredb()