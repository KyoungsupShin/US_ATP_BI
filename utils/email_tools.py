import pandas as pd
import numpy as np
from datetime import datetime
import win32com
import win32com.client
import re
import time
import itertools
import os
from difflib import SequenceMatcher
import xlrd
import urllib
from datetime import timedelta
from sqlalchemy import create_engine
from tqdm import tqdm
import warnings
from db_tools import *
from data_tools import *

class ETL_Pipelines(Warning_Utils, Check_Utils, Data_Manipulation, Data_Cleans):
    def get_attachment_info(self, saved_path='', mail_category='', check_date = datetime.now().strftime('%Y-%m-%d'), writer = 'SYSTEM'):
        self.saved_path = saved_path
        self.mail_category = mail_category
        self.check_date_sql = check_date
        self.writer = writer
        
    def read_attachment_excel(self, path = Global.root_path):
        try:
            print('[EVENT] EXCEL FILE IS READING. \n PATH: {}'.format(path))
            if path.split('.')[-1].lower() == 'xlsb':
                self.df = self.attachment_excel_clean(path,engine='pyxlsb', sheet_name=self.target_sheet_list[0])
            elif path.split('.')[-1].lower() == 'csv':
                self.df = pd.read_csv(path, encoding = 'cp949')                    
            else:
                self.df = self.attachment_excel_clean(path,engine='openpyxl', sheet_name=self.target_sheet_list[0])                        
            self.updated_iscolumn_check()
        except Exception as e:
            raise KeyError('ExtractDataError: WHILE READING EXCEL FILE ERROR OCCURED <br>' + str(e))

    def basic_process(self):
        self.meta_sheet_info()
        if self.mail_category == 'GA_PROD_PLAN':
            self.df = pd.read_excel(self.saved_path, engine='pyxlsb', sheet_name = 'Revised_Detail PSI (item code)', skiprows= 4)
            self.clean_prod_plan_data()
        else:
            self.read_attachment_excel(path = self.saved_path)
            self.clean_data_text(self.data_text)
            self.clean_data_int(self.data_int)
            self.clean_data_int(int)
            self.clean_data_float(self.data_float)
            self.clean_data_float(float)
            self.clean_data_datetime(self.yyyymmdd_datetime)
        self.Check_file_is_updated()
        self.data_null_check()
        self.insert_pd_tosql(self.mail_category, self.df, writer = True)
        # self.insert_dataframe_to_db(self.mail_category, self.df, writer = True)

class Outlook_Utils(ETL_Pipelines):
    def __init__(self, mail_receivers =  "digital_scm@us.q-cells.com"):
        super().__init__()
        self.mail_receivers = mail_receivers
        self.outlook = win32com.client.Dispatch("Outlook.Application")
        self.Rxoutlook = self.outlook.GetNamespace("MAPI")
        self.recip = self.Rxoutlook.CreateRecipient(self.mail_receivers)
        self.similarity_threshold = 0.5

    def send_email(self, mail_title, content_title, content_body, destination = None, appendix = '', attachment_path='', warning=False, excel_name='SYSTEM', RnRs = ['DEV'], critical=False):    
        # destination = 'dany.shin@hanwha.com' #only send the mail to developer 
        try:
            if critical == True:
                with open("../src/template_critical.html", "r", encoding='utf-8') as f:
                    text= f.read()
            else:
                with open("../src/template.html", "r", encoding='utf-8') as f:
                    text= f.read()

            text = text.replace('RPA-TITLE' , content_title)        
            text = text.replace('RPA-CONTENTS', content_body + appendix)        
            Txoutlook = self.outlook.CreateItem(0)

            if destination is None:
                Txoutlook.To = ';'.join(self.get_admin_address(RnRs).stack().tolist()) # Destination mail address list if there is RnR
                print('[EVENT] SENT MAIL TO {}'.format(';'.join(self.get_admin_address(RnRs).stack().tolist())))
            else:           
                Txoutlook.To = str(destination) # Destination mail address        
                print('[EVENT] SENT MAIL TO {}'.format(str(destination)))
            Txoutlook.Subject = mail_title
            Txoutlook.HTMLBody = f"""{text}"""
            if attachment_path:
                if type(attachment_path) == str:
                    Txoutlook.Attachments.Add(attachment_path)
                else:
                    for att in attachment_path:
                        Txoutlook.Attachments.Add(att)
            Txoutlook.Send()
            
            if warning == True:
                self.write_error_logs(error_name = content_title, error_type = content_body.split(':')[0], excel_name = excel_name)
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

    def invalidate_whitelist(self, mail_title, attch_name, mail_domain):
        whitelist_dict = self.fetch_data('select Mailtitle, Domain, ExcelName, MailCategory from MAIL_LIST ml')
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
        self.get_attachment_info(saved_path, self.mail_category_parse, save_date, writer = self.sender_addr.split('@')[0])
        self.basic_process()
        self.write_logs(self.att.FileName, 'PASS', self.i.SentOn.strftime('%Y-%m-%d %H:%M:%S'), self.mail_category_parse, self.sender_addr.split('@')[0])

