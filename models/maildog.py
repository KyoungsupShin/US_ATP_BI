import glob
import os
import sys
from datetime import datetime
sys.path.append('../utils')
from mail_detect import Email_detect
import threading
import time

if __name__ == '__main__':
    ed = Email_detect()
    try:
        while True:
            ed.rpa_email(check_sd = datetime.now().strftime('%Y-%m-%d'), 
                    download_filetype = ['xlsx', 'xlsb', 'xlsm', 'csv'], 
                    saveYN = True)
            time.sleep(30)
    except Exception as e:
        if str(e) != 'KeyboardInterrupt':
            ed.write_error_logs(error_name = 'RPAError: OUTLOOK MAIL BOX ERROR.' + str(e), 
                        error_type = 'RPAError')
    finally:
        ed.health_check_logs('RPA', 0)