from utils import *

class Email_detect(Email_Utils):
    def mail_attribute_check(self, download_filetype):
        atts = []
        for filetype in range(len(download_filetype)):
            atts.append([att for att in self.i.Attachments if download_filetype[filetype] in att.FileName.split('.')[-1].lower()]) #specific extension filtering
        self.atts = list(itertools.chain(*atts))                
        self.sender_addr = self.sender_mailaddr_extract(self.i)

    def request_master_check(self):
        print('[EVENT] RECEIVED REQUEST MASTER XLSX ATTACHMENTS')
        self.send_email('[RPA] MASTER FILE SHARING', 'MASTER FILE REQUEST'
                        ,'RETURNING MASTER EXCEL FILE'
                        ,destination = self.sender_mailaddr_extract(self.i) 
                        ,attachment_path = Global.root_path + '/data/master.xlsx')
        self.i.Delete()
    

    def request_data_check(self):
        try:
            self.mail_category = re.sub(r'[^a-zA-Z]', '', self.i.subject.lower().split('/')[1])                    
            self.category_replace()
            if len(self.i.subject.lower().split('/')) >= 3:
                etl_util = ETL_Utils('')
                Req_date = etl_util.yyyymmdd_datetime(self.i.subject.lower().split('/')[2])
            else:
                Req_date = datetime.now().strftime('%Y-%m-%d')
            Req_date_sql = f''' WHERE Updated_Date = '{Req_date}' '''
            
            sql = f'''SELECT * FROM {self.mail_category}''' + Req_date_sql
            
            self.connect_azuredb()
            df = self.fetch_data(sql)

            if len(df) > 0: #if data exists in DB
                df.to_csv(Global.root_path + '/data/{}.csv'.format(self.mail_category), index=False)
                print('[EVENT] RECEIVED REQUEST {} {} XLSX ATTACHMENTS'.format(self.mail_category, Req_date))
                self.send_email('[RPA] {} FILE SHARING'.format(self.mail_category)
                                ,'{} FILE REQUEST'.format(self.mail_category)
                                ,'RETURNING {} EXCEL FILE'.format(self.mail_category)
                                ,destination = self.sender_mailaddr_extract(self.i)
                                ,attachment_path=Global.root_path + '/data/{}.csv'.format(self.mail_category))
            else: # no data exists in DB
                print('[EVENT] RECEIVED REQUEST {} XLSX ATTACHMENTS, BUT NO UPDATED DATA'.format(self.mail_category))
                self.send_email('[RPA] {} FILE SHARING'.format(self.mail_category)
                                ,'{} FILE REQUEST'.format(self.mail_category)
                                ,'THERE IS NO UPDATED FILE : {}'.format(self.mail_category)
                                ,destination = self.sender_mailaddr_extract(self.i))
            self.i.Delete()
        except:
                print('[ERROR] RECEIVED REQUEST {} XLSX ATTACHMENTS, BUT ERROR OCCURRED'.format(self.mail_category))
                self.send_email('[ERROR] {} FILE SHARING'.format(self.mail_category)
                                ,'{} FILE REQUEST'.format(self.mail_category)
                                , 'RPA WOULD LIKE TO RECIVE THIS FORMAT <br> EX) request data/inbound/20220101 <br> PLEASE CHECK OUT MAIL TITLE FORMAT'
                                ,destination = self.sender_mailaddr_extract(self.i))
                pass

    def update_data_check(self):
        try:
            self.mail_category = re.sub(r'[^a-zA-Z]', '', self.i.subject.lower().split('/')[1])  
            self.category_replace()
            if len(self.i.subject.lower().split('/')) >= 3:
                etl_util = ETL_Utils('')
                Req_date = etl_util.yyyymmdd_datetime(self.i.subject.lower().split('/')[2])
            else:
                Req_date = datetime.now().strftime('%Y-%m-%d')
                    
            self.check_isadmin()
            if self.checkisadmin.values[0][0] == 1:
                print('\n' + 'Manual Update ' + '=' * 10, self.i.SentOn.strftime('%Y-%m-%d'), '=' * 10)
                print(self.i.subject) # mail title
                print(self.i.Sender, self.sender_addr, self.i.CC) #mail sender
                for att in self.atts:
                    self.save_attachments(att, i, Req_date)
                    self.write_logs(att.FileName, 'PASS', self.i.SentOn.strftime('%Y-%m-%d %H:%M:%S'))
                
                self.i.Move([i for i in self.Rxoutlook.Folders if str(i) == 'ATP_ATTACHMENTS'][0])
        except:
                print('[ERROR] RECEIVED REQUEST {} XLSX ATTACHMENTS, BUT ERROR OCCURRED'.format(self.mail_category))
                self.send_email('[ERROR] {} FILE SHARING'.format(self.mail_category)
                                ,'{} FILE REQUEST'.format(self.mail_category)
                                , 'RPA WOULD LIKE TO RECIVE THIS FORMAT <br> EX) request data/inbound/20220101 <br> PLEASE CHECK OUT MAIL TITLE FORMAT'
                                ,destination = self.sender_mailaddr_extract(self.i))
                pass
    def reset_master_check(self):
        print('[EVENT] RECEIVED RESET MASTER XLSX ATTACHMENTS')
        self.check_isadmin()
        if self.checkisadmin.values[0][0] == 1:
            for att in self.atts:
                if att.FileName == 'master.xlsx':
                    att.SaveAsFile(Global.root_path + '/data/' + att.FileName) # saving Master file                                        
                    try:
                        self.master_reset_main(self.sender_addr)
                        os.makedirs(Global.root_path + '/data/MASTER_HIST', exist_ok = True)
                        att.SaveAsFile(Global.root_path + '/data/MASTER_HIST/' + self.i.SentOn.strftime('%Y%m%d%H%M%S') + '_' + att.FileName) #saving Backup file
                        self.send_email('[RPA] MASTER FILE RESET RESULT', 'MASTER RESET'
                                        , 'RPA SYSTEM USED THIS FILE. YOUR REQUEST SUCCESSFULLY APLLIED <br> SENDING YOU THE NEWEST MASTER FILE'
                                        ,destination = self.sender_mailaddr_extract(self.i)
                                        ,attachment_path = Global.root_path + '/data/master.xlsx')
                        self.write_logs('MASTER', 'PASS', self.i.SentOn.strftime('%Y-%m-%d %H:%M:%S'))
                    except Exception as e:
                        print('[WARNING] MASTER FILE IS DAMAGED. RPA WILL ROLL-BACK.' , '\n', str(e))
                        self.master_reset_main(self.sender_addr, file_path = glob.glob(Global.root_path + '/data/MASTER_HIST/*.xlsx')[-1])
                        self.send_email('[ERROR] MASTER FILE RESET RESULT', 'MASTER RESET ERROR'
                                        ,'MASTER FILE YOU WOULD LIKE TO RESET SEEMS DAMAGED. <br> PLEASE CHECK OUT THE FILE AGAIN'
                                        ,destination = self.sender_mailaddr_extract(self.i))
        else:
            print('[WARNING] send e-mail that rejected due to low-authorization')
            self.send_email('[WARNING] MASTER FILE RESET RESULT', 'MASTER RESET DENIED'
                            ,'YOUR MAIL ADDRESS IS NOT AUTHORIZED. PLEASE RESISTER YOUR MAIL AS A MANAGER'
                            ,destination = self.sender_mailaddr_extract(self.i))
        self.i.Delete()  

    def normal_attach_check(self, saveYN):
        print('\n' + '=' * 10, self.i.SentOn.strftime('%Y-%m-%d'), '=' * 10)
        print(self.i.subject) # mail title
        print(self.i.Sender, self.sender_addr, self.i.CC) #mail sender
        for att in self.atts:
            if saveYN == True:
                self.save_attachments(att, self.i)
                self.write_logs(att.FileName, 'PASS', self.i.SentOn.strftime('%Y-%m-%d %H:%M:%S'))
        self.i.Move([i for i in self.Rxoutlook.Folders if str(i) == 'ATP_ATTACHMENTS'][0])

    def rpa_email(self, check_sd, download_filetype, saveYN):
        self.access_mailbox()
        print('[ITERATION] TOTAL E-MAIL FILED: {}'.format(len(self.inbox.items)))
        for self.i in self.inbox.items: #inbox mail iteration
            self.mail_attribute_check(download_filetype)
            try:
                if datetime.strptime(self.i.SentOn.strftime('%Y-%m-%d'), '%Y-%m-%d') >= datetime.strptime(check_sd, '%Y-%m-%d'):
                    if self.i.subject.lower().strip() == 'request master':
                        self.request_master_check()
                        continue

                    elif self.i.subject.lower().split('/')[0].strip() == 'request data':
                        self.request_data_check()
                        continue

                    else:
                        if len(self.atts) > 0: #attachment over 1
                            similarity, isdomainsame, self.mail_category = self.invalidate_whitelist(self.i.subject, self.sender_addr)
                            if self.i.subject.lower().split('/')[0].strip() == 'update data':
                                self.update_data_check()
                                continue

                            elif self.i.subject.lower() == 'reset master':
                                self.reset_master_check()
                                continue

                            elif (similarity > self.similarity_threshold) & (isdomainsame == True):
                                self.normal_attach_check(saveYN)
                                continue                    
                else:
                    self.i.Delete()
            except Exception as e:
                print(e)
                self.i.Delete()
                pass 
        print('[ITERATION] INBOX CHECKING JUST DONE')

if __name__ == '__main__':
    ed = Email_detect()
    ed.rpa_email(check_sd = datetime.now().strftime('%Y-%m-%d'), 
                download_filetype = ['xlsx', 'xlsb', 'xlsm', 'csv'], 
                saveYN = True)