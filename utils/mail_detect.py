from utils import *
from sap_batch import *
import glob
from datetime import datetime 

class Email_detect(Email_Utils):
    def request_master_check(self):
        print('[EVENT] RECEIVED REQUEST MASTER XLSX ATTACHMENTS')
        self.excel_name = 'MASTER'
        self.send_email('[RPA] MASTER FILE SHARING', 'MASTER FILE REQUEST'
                        ,'RETURNING MASTER EXCEL FILE'
                        ,destination = self.sender_mailaddr_extract(self.i)
                        ,attachment_path = [Global.root_path + '/data/master.xlsx',
                                            Global.root_path + '/data/WH_Master.csv'])
        self.read_qspdb(save_YN = True)
        self.i.Delete()

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
                        raise ValueError('RPAError:DURING RESETTING MASTER DATA, SOMETHING WENT WRONG. \n', str(e))
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

    def sap_batch(self):
        print('[EVENT] RECEIVED REQUEST MANUAL BATCH UPDATE.')
        smr = SAP_Master_Reset()
        smr.read_qspdb()
        smr.update_sap_data()
        smr.atp_raw_history_batch()
        smr.atp_batch()
        smr.atp_ending_onhand_batch()
        del smr 
        self.i.Delete()

    def rpa_email(self, check_sd, download_filetype, saveYN):
        try:
            self.access_mailbox()
        except Exception as e:
            self.send_email('[EMAIL APP ERROR] {}'.format(str(e)[1:-1].split(':')[0])
                            ,'ERROR MESSAGE'
                            , str(e)[1:-1]
                            , RnRs=['PLAN', 'DEV'] )
            self.write_error_logs(error_name = str(e)[1:-1], error_type = str(e)[1:-1].split(':')[0])

        for self.i in self.inbox.items: #inbox mail iteration
            try:
                self.mail_target_extension_filter(download_filetype)
            except Exception as e:
                print('Untitled Outlook mail will be deleted automatically. ', str(e))
                self.i.Delete()
                continue
            try:
                if (datetime.now() - datetime.strptime(self.i.SentOn.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')).days < 1:
                    if self.i.subject.lower().strip() == 'request master':
                        self.request_master_check()
                        continue
                    elif self.i.subject.lower().strip() == 'request batch':
                        self.sap_batch()
                        continue
                    elif self.i.subject.lower().strip() == 'request atp':
                        print('[EVENT] RECEIVED REQUEST ATP BI BATCH RAW DATASET.')
                        self.connect_azuredb()
                        df = self.fetch_data(sql = 'select * from ATP_BI')
                        df = df.rename(columns={"기준일자":"Updated_Date", "날짜":"ATP_Date", "제품명" : "Product_Name"})
                        df.to_csv(Global.root_path + '/data/dummy/atp.csv', encoding='utf-8-sig', index = None)
                        self.send_email('[RPA] ATP_BI TABLE RAW DATA SHARING'
                                        ,'ATP_BI TABLE RAW DATA'
                                        ,'SENDING YOU THE NEWEST ATP BI CSV FILE'
                                        ,destination = self.sender_mailaddr_extract(self.i)
                                        ,attachment_path = Global.root_path + '/data/dummy/atp.csv')
                        self.i.Delete()
                        continue
                    else:
                        if len(self.atts) > 0: #attachment over 1
                            if self.i.subject.lower() == 'reset master':
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
                                            ,self.i.SentOn.strftime('%Y-%m-%d %H:%M:%S') + '<br>' + str(self.i.subject) + ' / ' + str(self.i.Sender) +  '<br>' + str(self.att.FileName) + '<br>' + str(e) 
                                            , RnRs = ['PLAN', 'DEV']
                                            )
                                        print('[WARNING] THIS ATTACHMENT HAS AN ERROR.')
                                        self.write_logs(self.att.FileName, 'FAIL', self.i.SentOn.strftime('%Y-%m-%d %H:%M:%S'), self.mail_category_parse, self.sender_addr.split('@')[0])
                else:
                    self.i.Delete()

            except Exception as e:
                self.send_email('[EMAIL MODULE ERROR] {}'.format(str(e)[1:-1].split(':')[0])
                                ,'ERROR MESSAGE'
                                , str(e)[1:-1]
                                , RnRs=['PLAN', 'DEV'])
                self.write_error_logs(error_name = str(e)[1:-1], error_type = str(e)[1:-1].split(':')[0],
                                        Mail_Sender = self.sender_mailaddr_extract(self.i), excel_name=self.excel_name)
        self.health_check_logs('RPA', 1)            
        print('[ITERATION] INBOX CHECKING JUST DONE')

if __name__ == '__main__':
    ed = Email_detect()
    ed.rpa_email(check_sd = datetime.now().strftime('%Y-%m-%d'), 
                download_filetype = ['xlsx', 'xlsb', 'xlsm', 'csv'], 
                saveYN = True)
        