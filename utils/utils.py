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

class DB_Utils():
    def connect_azuredb(self):
        server = 'us-qcells-atp-db-server.database.windows.net'
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
        self.conn.close()

        row = [list(i) for i in row]
        col_names = [item[0] for item in self.cursor.description]
        return pd.DataFrame(row, columns=col_names)

    def delete_data(self, sql):
        self.cursor.execute(sql)
        self.conn.commit()
        self.conn.close()

    def insert_data(self, sql, df):
        for row in range(len(df)):
            time.sleep(0.02)
            print(df.values[row])   
            self.cursor.execute(sql, tuple(df.values[row]))
        self.conn.commit()
        self.conn.close()

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
        
class Master_Reset(DB_Utils):
    def __init__(self):
        self.connect_azuredb()
        self.master_df = pd.ExcelFile('../data/master.xlsx',engine='openpyxl')
        self.master_df.sheet_names #equal to DB table name     
        self.reset_master_tables()
        self.push_data()

    def reset_master_tables(self):
        for sheet in self.master_df.sheet_names:
            sql = 'DELETE FROM {}'.format(sheet)
            self.cursor.execute(sql)
        self.conn.commit()
    
    def push_data(self):
        df = self.master_df.parse(self.master_df.sheet_names[0])
        for i in range(len(df)):
            time.sleep(0.02)
            sql = f'''INSERT INTO {self.master_df.sheet_names[0]} (MailAddr, Division, RnR) VALUES 
            {tuple(df[['MailAddr','Division','RnR']].values[i])}'''
            self.cursor.execute(sql)
        self.conn.commit()

        df = self.master_df.parse(self.master_df.sheet_names[1])
        for i in range(len(df)):
            time.sleep(0.02)
            sql = f'''INSERT INTO {self.master_df.sheet_names[1]} (AREA, MailTitle, Domain, ExcelName, UseYN) VALUES 
            {tuple(df[['AREA', 'MailTitle', 'Domain', 'ExcelName', 'UseYN']].values[i])}'''
            self.cursor.execute(sql)
        self.conn.commit()

        df = self.master_df.parse(self.master_df.sheet_names[2])
        for i in range(len(df)):
            time.sleep(0.02)
            sql = f'''INSERT INTO {self.master_df.sheet_names[2]} (FullStatesName, CodeName, CodeNumber) VALUES 
            {tuple(df[['FullStatesName', 'CodeName', 'CodeNumber']].values[i])}'''
            self.cursor.execute(sql)
        self.conn.commit()

        df = self.master_df.parse(self.master_df.sheet_names[3])
        for i in range(len(df)):
            time.sleep(0.02)
            sql = f'''INSERT INTO {self.master_df.sheet_names[3]} (ExcelName, Domain, SheetList, TransactionColumn, TargetColumns, UseYN) VALUES 
            {tuple(df[['ExcelName', 'Domain', 'SheetList', 'TransactionColumn','TargetColumns','UseYN']].values[i])}'''
            self.cursor.execute(sql)
        self.conn.commit()

        df = self.master_df.parse(self.master_df.sheet_names[4])
        for i in range(len(df)):
            time.sleep(0.02)
            sql = f'''INSERT INTO {self.master_df.sheet_names[4]} (WarehouseName, AREA, POD, StreetAddr, CityName, StatesName, PostalCode, FullAddress, UseYN) VALUES 
            {tuple(df[['WarehouseName', 'AREA', 'POD', 'StreetAddr', 'CityName', 'StatesName', 'PostalCode', 'FullAddress', 'UseYN']].values[i])}'''
            self.cursor.execute(sql)
        self.conn.commit()
        print('RESET ALL MASTER DATA!!!')
        self.conn.close()

class Email_Utils(DB_Utils):
    def __init__(self, mail_receivers):
        super().__init__()
        self.mail_receivers = mail_receivers
        self.outlook = win32com.client.Dispatch("Outlook.Application")
        self.Rxoutlook = self.outlook.GetNamespace("MAPI")
        self.recip = self.Rxoutlook.CreateRecipient(self.mail_receivers)
        self.similarity_threshold = 0.5
        self.root_path = 'C:/Users/Qcells/Desktop/Digital_Planning/atp/script/data'

    def send_email(self, subject, body, destination, master = False):    
        Txoutlook = self.outlook.CreateItem(0)
        Txoutlook.To = destination
        Txoutlook.Subject = subject
        Txoutlook.HTMLBody = f"""{body}"""
        if master == True:
            Txoutlook.Attachments.Add(self.root_path + '/master.xlsx')
        Txoutlook.Send()    

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
        inbox = self.Rxoutlook.GetSharedDefaultFolder(self.recip, 6)
        print('TOTAL E-MAIL FILED: {}'.format(len(inbox.items)))

        for i in inbox.items: #inbox의 mail iteration
            atts = []
            if datetime.strptime(i.SentOn.strftime('%Y-%m-%d'), '%Y-%m-%d') >= datetime.strptime(check_sd, '%Y-%m-%d'): #YYYYMMDD 이전 메일 filtering out
                for filetype in range(len(download_filetype)):
                    atts.append([att for att in i.Attachments if download_filetype[filetype] in att.FileName.split('.')[-1].lower()]) #특정 확장자명 filtering
                atts = list(itertools.chain(*atts))
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
                                os.makedirs(self.root_path + '/MASTSER_HIST', exist_ok = True)
                                att.SaveAsFile(self.root_path + '/MASTSER_HIST/' + datetime.now().strftime('%Y%m%d%H%M%S') + '_' + att.FileName)
                                att.SaveAsFile(self.root_path + '/' + att.FileName)
                                self.send_email('[RPA] MASTER FILE RESET RESULT', 'This is the file used!'
                                                ,destination = self.sender_mailaddr_extract(i)
                                                ,master = True)
                                Master_Reset()
                        self.write_logs('MASTER', 'PASS')
                        i.Delete()
                        continue
                else:
                    if len(atts) > 0: #첨부파일이 1개 이상 메일만
                        try:
                            sender_addr = self.sender_mailaddr_extract(i)
                            similarity, isdomainsame = self.invalidate_whitelist(i.subject, sender_addr)
                            if (similarity > self.similarity_threshold) & (isdomainsame == True): #제목 유사도 0.9 이상, 도메인 동일 조건 filtering
                                print('=' * 10, i.SentOn.strftime('%Y-%m-%d'), '=' * 10)
                                print(i.subject) # 메일제목
                                print(i.Sender, sender_addr, i.CC) #메일 발신인
                                for att in atts:
                                    print(att.FileName) #메일 첨부파일    
                                    if saveYN == True:
                                        print('[EVENT] SAVED ATTACHMENTS')
                                        os.makedirs(self.root_path + datetime.now().strftime('%Y-%m-%d'), exist_ok = True)
                                        att.SaveAsFile(self.root_path + datetime.now().strftime('%Y-%m-%d') + '/' + att.FileName)
                                    self.write_logs(att.FileName, 'PASS')
                                i.Delete()
                                print('\n')  
                                continue       
                        except Exception as e:
                            print(e)
                            pass

