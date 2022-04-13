import glob
import os
import sys
from datetime import datetime
sys.path.append('../utils')
from utils import Email_Utils
import threading
import time

if __name__ == '__main__':
    email_rpa = Email_Utils(mail_receivers = "digital_scm@us.q-cells.com")
    while True:
        print('[ITERATION] {} MAIL RPA ON WORKING NOW'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        email_rpa.recevie_email(
                    check_sd = datetime.now().strftime('%Y-%m-%d'), 
                    download_filetype = ['xlsx', 'xlsb'], 
                    saveYN = True)
        time.sleep(60)

#Threading is not available

