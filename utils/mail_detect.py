from utils import *

class Email_detect(Email_Utils):
    def mail_attribute_check(self):
        atts = []
        for filetype in range(len(download_filetype)):
            atts.append([att for att in i.Attachments if download_filetype[filetype] in att.FileName.split('.')[-1].lower()]) #specific extension filtering
        atts = list(itertools.chain(*atts))                
        sender_addr = self.sender_mailaddr_extract(i)

    def request_master_check(self):
        print('[EVENT] RECEIVED REQUEST MASTER XLSX ATTACHMENTS')
        self.send_email('[RPA] MASTER FILE SHARING', 'MASTER FILE REQUEST'
                        ,'RETURNING MASTER EXCEL FILE'
                        ,destination = self.sender_mailaddr_extract(i) 
                        ,attachment_path = Global.root_path + '/data/master.xlsx')
        i.Delete()
    

    def request_data_check(self):
        self.mail_category = re.sub(r'[^a-zA-Z]', '', i.subject.lower().split('/')[1])                    
        self.mail_category = self.category_replace()

        if len(i.subject.lower().split('/')) >= 3:
            etl_util = ETL_Utils('')
            Req_date = etl_util.yyyymmdd_datetime(i.subject.lower().split('/')[2])
        else:
            Req_date = datetime.now().strftime('%Y-%m-%d')
        Req_date_sql = f''' WHERE Updated_Date = '{Req_date}' '''
        
        sql = f'''SELECT * FROM {self.mail_category}''' + Req_date_sql
        self.connect_azuredb()
        df = self.fetch_data(sql)

        if len(df) > 0: 
            df.to_csv(Global.root_path + '/data/{}.csv'.format(self.mail_category), index=False)
            print('[EVENT] RECEIVED REQUEST {} {} XLSX ATTACHMENTS'.format(self.mail_category, Req_date))
            self.send_email('[RPA] {} FILE SHARING'.format(self.mail_category)
                            ,'{} FILE REQUEST'.format(self.mail_category)
                            ,'RETURNING {} EXCEL FILE'.format(self.mail_category)
                            ,destination = self.sender_mailaddr_extract(i)
                            ,attachment_path=Global.root_path + '/data/{}.csv'.format(self.mail_category))
        else:
            print('[EVENT] RECEIVED REQUEST {} XLSX ATTACHMENTS, BUT NO UPDATED DATA'.format(self.mail_category))
            self.send_email('[RPA] {} FILE SHARING'.format(self.mail_category)
                            ,'{} FILE REQUEST'.format(self.mail_category)
                            ,'THERE IS NO UPDATED FILE : {}'.format(self.mail_category)
                            ,destination = self.sender_mailaddr_extract(i))
        i.Delete()

    def update_data_check(self):
        self.mail_category = re.sub(r'[^a-zA-Z]', '', i.subject.lower().split('/')[1])  
        self.mail_category = self.category_replace()
        if len(i.subject.lower().split('/')) >= 3:
            etl_util = ETL_Utils('')
            Req_date = etl_util.yyyymmdd_datetime(i.subject.lower().split('/')[2])
        else:
            Req_date = datetime.now().strftime('%Y-%m-%d')
                
        self.check_isadmin(sender_addr)
        if self.checkisadmin.values[0][0] == 1:
            print('\n' + 'Manual Update ' + '=' * 10, i.SentOn.strftime('%Y-%m-%d'), '=' * 10)
            print(i.subject) # mail title
            print(i.Sender, sender_addr, i.CC) #mail sender
            for att in atts:
                self.save_attachments(att, i, Req_date)
                self.write_logs(att.FileName, 'PASS', i.SentOn.strftime('%Y-%m-%d %H:%M:%S'))
            
            i.Move([i for i in self.Rxoutlook.Folders if str(i) == 'ATP_ATTACHMENTS'][0])

    def reset_master_check(self):
        print('[EVENT] RECEIVED RESET MASTER XLSX ATTACHMENTS')
        self.check_isadmin(sender_addr)
        if self.checkisadmin.values[0][0] == 1:
            for att in atts:
                if att.FileName == 'master.xlsx':
                    att.SaveAsFile(Global.root_path + '/data/' + att.FileName) # saving Master file                                        
                    try:
                        self.master_reset_main(sender_addr)
                        os.makedirs(Global.root_path + '/data/MASTER_HIST', exist_ok = True)
                        att.SaveAsFile(Global.root_path + '/data/MASTER_HIST/' + i.SentOn.strftime('%Y%m%d%H%M%S') + '_' + att.FileName) #saving Backup file
                        self.send_email('[RPA] MASTER FILE RESET RESULT', 'MASTER RESET'
                                        , 'RPA SYSTEM USED THIS FILE. YOUR REQUEST SUCCESSFULLY APLLIED <br> SENDING YOU THE NEWEST MASTER FILE'
                                        ,destination = self.sender_mailaddr_extract(i)
                                        ,attachment_path = Global.root_path + '/data/master.xlsx')
                        self.write_logs('MASTER', 'PASS', i.SentOn.strftime('%Y-%m-%d %H:%M:%S'))
                    except Exception as e:
                        print('[WARNING] MASTER FILE IS DAMAGED. RPA WILL ROLL-BACK.' , '\n', str(e))
                        self.master_reset_main(sender_addr, file_path = glob.glob(Global.root_path + '/data/MASTER_HIST/*.xlsx')[-1])
                        self.send_email('[ERROR] MASTER FILE RESET RESULT', 'MASTER RESET ERROR'
                                        ,'MASTER FILE YOU WOULD LIKE TO RESET SEEMS DAMAGED. <br> PLEASE CHECK OUT THE FILE AGAIN'
                                        ,destination = self.sender_mailaddr_extract(i))
        else:
            print('[WARNING] send e-mail that rejected due to low-authorization')
            self.send_email('[WARNING] MASTER FILE RESET RESULT', 'MASTER RESET DENIED'
                            ,'YOUR MAIL ADDRESS IS NOT AUTHORIZED. PLEASE RESISTER YOUR MAIL AS A MANAGER'
                            ,destination = self.sender_mailaddr_extract(i))
        i.Delete()  

    def normal_attach_check(self):
        print('\n' + '=' * 10, i.SentOn.strftime('%Y-%m-%d'), '=' * 10)
        print(i.subject) # mail title
        print(i.Sender, sender_addr, i.CC) #mail sender
        for att in atts:
            if saveYN == True:
                self.save_attachments(att, i)
                self.write_logs(att.FileName, 'PASS', i.SentOn.strftime('%Y-%m-%d %H:%M:%S'))
        i.Move([i for i in self.Rxoutlook.Folders if str(i) == 'ATP_ATTACHMENTS'][0])


    def recevie_email(self, check_sd, download_filetype, saveYN):
        #TDD download email data, request master, request data(previous, today), reset master, reset data(previous, today) 
        print('[ITERATION] TOTAL E-MAIL FILED: {}'.format(len(inbox.items)))
        for i in inbox.items: #inbox mail iteration
            self.mail_attribute_check()
            # try:
            if datetime.strptime(i.SentOn.strftime('%Y-%m-%d'), '%Y-%m-%d') >= datetime.strptime(check_sd, '%Y-%m-%d'): #YYYYMMDD previous mail filtering out
                if i.subject.lower().strip() == 'request master':
                    self.request_master_check()
                    continue

                elif i.subject.lower().split('/')[0].strip() == 'request data':
                    try:
                        self.request_data_check()
                        continue
                    except:
                            print('[ERROR] RECEIVED REQUEST {} XLSX ATTACHMENTS, BUT ERROR OCCURRED'.format(self.mail_category))
                            self.send_email('[ERROR] {} FILE SHARING'.format(self.mail_category)
                                            ,'{} FILE REQUEST'.format(self.mail_category)
                                            , 'RPA WOULD LIKE TO RECIVE THIS FORMAT <br> EX) request data/inbound/20220101 <br> PLEASE CHECK OUT MAIL TITLE FORMAT'
                                            ,destination = self.sender_mailaddr_extract(i))
                            pass
                else:
                    if len(atts) > 0: #attachment over 1
                        similarity, isdomainsame, self.self.mail_category = self.invalidate_whitelist(i.subject, sender_addr)
                        # 날짜 추출을 dataframe의 update_date로 변경 // excel file encrypt error 처리 
                        # 현재 request -> data save -> etl pipeline -> delete prev -> insert에서 updated_date column을 포함하지 않고 insert함.
                        #check if updated -> delete data -> insert query

                        if i.subject.lower().split('/')[0].strip() == 'update data':
                            self.update_data_check()
                            continue

                        elif i.subject.lower() == 'reset master':
                            self.reset_master_check()
                            continue

                        elif (similarity > self.similarity_threshold) & (isdomainsame == True): #title ssim over 0.9, domain filtering
                            self.normal_attach_check()
                            continue                    
            else:
                i.Delete()
            # except Exception as e:
            #     print(e)
            #     i.Delete()
            #     pass 
        print('[ITERATION] INBOX CHECKING JUST DONE')
