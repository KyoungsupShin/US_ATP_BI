import sys
sys.path.append('../utils/')
from utils import *
from sap_sqls import *
from sap_batch import * 
from ending_onhand import * 
import datetime
import threading
import time
import pandas as pd

if __name__ == '__main__':
    smr = SAP_Master_Reset()
    try:
        while True:
            smr.health_check_logs('BATCH', 1)
            if int(datetime.datetime.now().hour) == 4 and int(datetime.datetime.now().minute) == 2:
                smr.read_qspdb()
                smr.update_sap_data()
                smr.atp_raw_history_batch()
                smr.atp_batch()
                smr.atp_ending_onhand_batch()
            else:
                print('[ITERATION] SYNC MODULE IS WAITING FOR NEXT BATCH')
            time.sleep(59)
    except Exception as e:
        print(e)
        rollbackyn = input('BATCH PROCESS GOT AN ERROR. YOU WANT TO ROLLBACK?(Y/N)')
        if rollbackyn.lower() == 'y':
            for view in smr.view_names:
                smr.initial_history_table(view[1])
        smr.write_error_logs(error_name = str(e)[1:-1], error_type = str(e)[1:-1].split(':')[0], excel_name= smr.excel_name)
    finally:
       smr.health_check_logs('BATCH', 0)    
       del smr 