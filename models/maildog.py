import glob
import os
import sys
from datetime import datetime
sys.path.append('../utils')
from utils import Email_Utils

if __name__ == '__main__':
    email_rpa = Email_Utils(mail_receivers = "dany.shin@hanwha.com")
    email_rpa.recevie_email(
                check_sd = datetime.now().strftime('%Y-%m-%d'), 
                download_filetype = ['xlsx', 'xlsb'], 
                saveYN = True)