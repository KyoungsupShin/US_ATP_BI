from utils import *
import glob

class Email_detect(Email_Utils):
    def request_master_check(self):
        print('[EVENT] RECEIVED REQUEST MASTER XLSX ATTACHMENTS')
        self.excel_name = 'MASTER'
        self.send_email('[RPA] MASTER FILE SHARING', 'MASTER FILE REQUEST'
                        ,'RETURNING MASTER EXCEL FILE'
                        ,attachment_path = Global.root_path + '/data/master.xlsx')
        self.i.Delete()
    
    def request_data_check(self):
        print('[EVENT] RECEIVED REQUEST XLSX DATA ATTACHMENTS')
        self.extract_request_update_mail_info()  
        Req_date_sql = f''' WHERE Updated_Date = '{self.Req_date}' '''
        self.excel_name = self.mail_category_parse
        sql = f'''SELECT * FROM {self.mail_category_parse}''' + Req_date_sql
        self.connect_azuredb()
        df = self.fetch_data(sql)
        if len(df) > 0: #if data exists in DB, save backup data as csv file
            save_path = Global.root_path + '/data/{}/{}_{}.csv'.format(self.Req_date, self.mail_category_parse,datetime.now().strftime('%Y%m%d%H%M%S'))
            df.to_csv(save_path, index=False)          
            print('[EVENT] RECEIVED REQUEST {} {} XLSX ATTACHMENTS'.format(self.mail_category_parse, self.Req_date))
            self.send_email('[RPA] {} FILE SHARING'.format(self.mail_category_parse)
                            ,'{} FILE REQUEST'.format(self.mail_category_parse)
                            ,'RETURNING {} EXCEL FILE'.format(self.mail_category_parse)
                            ,attachment_path=save_path)
        else: # no data exists in DB
            print('[EVENT] RECEIVED REQUEST {} XLSX ATTACHMENTS, BUT NO UPDATED DATA'.format(self.mail_category_parse))
            self.send_email('[RPA] {} FILE SHARING'.format(self.mail_category_parse)
                            ,'{} FILE REQUEST'.format(self.mail_category_parse)
                            ,'THERE IS NO UPDATED FILE : {}'.format(self.mail_category_parse))
        self.i.Delete()

    def update_data_check(self): 
        print('[EVENT] RECEIVED UPDATE REQUEST')
        try:
            self.excel_name=self.mail_category_parse
            self.extract_request_update_mail_info()  
            self.check_isadmin()
            if self.checkisadmin.values[0][0] == 1:
                print('[EVENT] Manual Update => TARGET: {} / DATE {}'.format(self.mail_category_parse, self.Req_date))
                for att in self.atts:
                    # self.save_attachments(att, self.i, self.Req_date)
                    self.write_logs(att.FileName, 'PASS', self.i.SentOn.strftime('%Y-%m-%d %H:%M:%S'), self.mail_category_parse, self.sender_addr.split('@')[0])                
                self.i.Move([i for i in self.Rxoutlook.Folders if str(i) == 'ATP_ATTACHMENTS'][0])
        except:
            raise ValueError('RPAError:DURING UPDATEING DATA, SOMETHING WENT WRONG.')

    def reset_master_check(self):
        print('[EVENT] RECEIVED RESET MASTER XLSX ATTACHMENTS')
        self.check_isadmin()
        if self.checkisadmin.values[0][0] == 1:
            for att in self.atts:
                if att.FileName == 'master.xlsx':
                    self.excel_name = 'MASTER'
                    att.SaveAsFile(Global.root_path + '/data/' + att.FileName) # saving Master file                                        
                    try:
                        self.master_reset_main(self.sender_addr)    
                        os.makedirs(Global.root_path + '/data/MASTER_HIST', exist_ok = True)
                        att.SaveAsFile(Global.root_path + '/data/MASTER_HIST/' + self.i.SentOn.strftime('%Y%m%d%H%M%S') + '_' + att.FileName) #saving Backup file
                        self.send_email('[RPA] MASTER FILE RESET RESULT'
                                        ,'MASTER RESET'
                                        ,'RPA SYSTEM USED THIS FILE. YOUR REQUEST SUCCESSFULLY APLLIED <br> SENDING YOU THE NEWEST MASTER FILE'
                                        ,destination = self.sender_mailaddr_extract(self.i)
                                        ,attachment_path = Global.root_path + '/data/master.xlsx')
                        self.write_logs('MASTER', 'PASS', self.i.SentOn.strftime('%Y-%m-%d %H:%M:%S'), 'MASTER', self.sender_addr.split('@')[0])
                    except Exception as e:
                        print('[WARNING] MASTER FILE IS DAMAGED. RPA WILL ROLL-BACK.' , '\n', str(e))
                        self.master_reset_main(self.sender_addr, file_path = glob.glob(Global.root_path + '/data/MASTER_HIST/*.xlsx')[-1])
                        raise ValueError('RPAError:DURING RESETTING MASTER DATA, SOMETHING WENT WRONG.')
        else:
            print('[WARNING] SEND E-MAIL. YOUR REQUEST REJECTED DUE TO LOW-AUTHORIZATION')
            raise ValueError('MasterError:SEND E-MAIL. YOUR REQUEST REJECTED DUE TO LOW-AUTHORIZATION.')
        self.i.Delete()  

    def normal_attach_check(self, saveYN):
        print('\n' + '=' * 10, self.i.SentOn.strftime('%Y-%m-%d'), '=' * 10)
        print(self.i.subject) # mail title
        print(self.i.Sender, self.sender_addr, self.i.CC) #mail sender
        if self.mail_moved_YN == False:
            self.i.Move([i for i in self.Rxoutlook.Folders if str(i) == 'ATP_ATTACHMENTS'][0])
            self.mail_moved_YN = True
        if saveYN == True:
            self.excel_name=self.mail_category_parse
            self.save_attachments(self.att, self.i)

    def rpa_email(self, check_sd, download_filetype, saveYN):
        try:
            self.access_mailbox()
        except Exception as e:
            self.send_email('[EMAIL APP ERROR] {}'.format(str(e)[1:-1].split(':')[0])
                            ,'ERROR MESSAGE'
                            , str(e)[1:-1])
            self.write_error_logs(error_name = str(e)[1:-1], error_type = str(e)[1:-1].split(':')[0])

        for self.i in self.inbox.items: #inbox mail iteration
            self.mail_target_extension_filter(download_filetype)
            try:
                if (datetime.now() - datetime.strptime(self.i.SentOn.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')).days < 1:
                    if self.i.subject.lower().strip() == 'request master':
                        self.request_master_check()
                        continue
                    elif self.i.subject.lower().split('/')[0].strip() == 'request data':
                        self.request_data_check()
                        continue
                    else:
                        if len(self.atts) > 0: #attachment over 1
                            if self.i.subject.lower().split('/')[0].strip() == 'update data':
                                self.update_data_check()
                                continue
                            elif self.i.subject.lower() == 'reset master':
                                self.reset_master_check()
                                continue
                            else:
                                self.mail_moved_YN = False
                                for self.att in self.atts:
                                    try:
                                        similarity, isdomainsame, self.mail_category_parse = self.invalidate_whitelist(self.i.subject, self.att.FileName, self.sender_addr)
                                        if (similarity > self.similarity_threshold) & (isdomainsame == True):
                                            if self.mail_category_parse != 'None':
                                                self.normal_attach_check(saveYN)
                                                continue                    
                                    except Exception as e:
                                        self.send_email('[RPA FILE ERROR] {}'.format(str(e).split(':')[0])
                                            ,'ERROR MESSAGE'
                                            ,self.i.SentOn.strftime('%Y-%m-%d %H:%M:%S') + '<br>' + str(self.i.subject) + ' / ' + str(self.i.Sender) +  '<br>' + str(self.att.FileName) + '<br>' + str(e) )
                                        print('[WARNING] THIS ATTACHMENT HAS AN ERROR.')
                                        self.write_logs(self.att.FileName, 'FAIL', self.i.SentOn.strftime('%Y-%m-%d %H:%M:%S'), self.mail_category_parse, self.sender_addr.split('@')[0])
                else:
                    self.i.Delete()

            except Exception as e:
                self.send_email('[EMAIL MODULE ERROR] {}'.format(str(e)[1:-1].split(':')[0])
                                ,'ERROR MESSAGE'
                                , str(e)[1:-1])
                self.write_error_logs(error_name = str(e)[1:-1], error_type = str(e)[1:-1].split(':')[0],
                                        Mail_Sender = self.sender_mailaddr_extract(self.i), excel_name=self.excel_name)
        self.health_check_logs('RPA', 1)            
        print('[ITERATION] INBOX CHECKING JUST DONE')

if __name__ == '__main__':
    ed = Email_detect()
    ed.rpa_email(check_sd = datetime.now().strftime('%Y-%m-%d'), 
                download_filetype = ['xlsx', 'xlsb', 'xlsm', 'csv'], 
                saveYN = True)
        