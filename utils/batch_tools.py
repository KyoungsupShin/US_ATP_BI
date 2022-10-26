import sys
sys.path.append('../src')
from email_tools import *
from sap_sqls import *
from atp_tools import * 
from pbixapi_tools import * 
from datetime import datetime
import threading
import time
import pandas as pd

class SAP_Master_Reset(Ending_On_Hand):    
    def atp_raw_history_batch(self, isnullcheck = True):# [call sql table, save sql table, check column names]
        self.view_names = [['ATP_OUTBOUND' , 'OUT_BOUND_HISTORY', ['Product_Name', 'WH_Name'], ['OSR', '3PL_OUTBOUND', 'DEV', 'PLAN']]
                        , ['ATP_ONHAND', 'ON_HAND_HISTORY', ['Product_Name', 'WH_Name'], ['SAP', 'DEV', 'PLAN']]
                        , ['ATP_INBOUND_DOMESTIC', 'INBOUND_DOMESTIC_HISTORY', ['Product_Name', 'WH_Name'], ['SAP', 'DEV', 'PLAN']]
                        , ['ATP_INBOUND_OVERSEA', 'INBOUND_OVERSEA_HISTORY', ['Product_Name'], ['SAP', 'DEV', 'PLAN']]
                        , ['ATP_DELIVERY_STATUS', 'DELIVERY_STATUS_HISTORY', ['Product_Name', 'WH_Name'], ['SAP', 'DEV', 'PLAN']]
                        ]
        
        for name in self.view_names:
            self.excel_name = name[1]        
            print('[EVENT] STARTING TO SAVE TODAY HISTORY {} RESULT'.format(name[0]))
            if name[0] == 'ATP_OUTBOUND':
                self.df = self.fetch_data('''SELECT * FROM ATP_OUTBOUND 
                                             WHERE Logi_Status not like 'Can%' OR CPO_Status NOT IN ('CANCELED', 'CONSUMED')''')
            else:      
                self.df = self.fetch_data('select * from {}'.format(name[0]))
            if isnullcheck == True:
                self.atp_data_null_check(name[0], name[2], name[3])
            self.check_batch_is_updated(name[1])        
            self.insert_pd_tosql(name[1], self.df)
            self.write_logs(name[1], 'PASS', datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'HISTORY')

    def atp_batch(self):
        print('[EVENT] STARTING TO SAVE TODAY HISTORY ATP RESULT')
        self.excel_name = 'ATP_BATCH_HISTORY'
        self.df_atp = self.fetch_data(sql_atp)
        self.df_atp = self.df_atp.fillna('')
        self.check_batch_is_updated('ATP_HISTORY')
        self.insert_pd_tosql('ATP_HISTORY', self.df_atp)
        self.write_logs('ATP_HISTORY', 'PASS', datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'HISTORY')

    def atp_ending_onhand_batch(self):
        print('[EVENT] STARTING TO SAVE TODAY HISTORY ATP ENDING ONHAND RESULT')
        self.excel_name = 'ATP_BATCH_HISTORY'
        df_ending_onhand_result_join = self.Ending_onhand_main()
        self.check_batch_is_updated('ATP_BI_ENDING_ONHAND')
        self.insert_pd_tosql('ATP_BI_ENDING_ONHAND', df_ending_onhand_result_join)
        self.write_logs('ATP_BI_ENDING_ONHAND', 'PASS', datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'HISTORY')
        self.pbix_api_refresh()
        
    def pbix_api_refresh(self):
        rbwa = Refresh_pbix_web_api()
        rbwa.request_refresh()

if __name__ == '__main__':
    smr = SAP_Master_Reset()
    # smr.read_qspdb() # daily batch
    # smr.insert_sap_data_to_db() # daily batch
    # smr.atp_raw_history_batch(isnullcheck = True) # daily batch 
    # smr.atp_batch() # daily batch
    smr.atp_ending_onhand_batch() # event batch -> manual email ?
    # smr.atp_data_check() # daily batch 
    # PR validation add