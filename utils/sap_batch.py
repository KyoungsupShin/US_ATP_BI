import sys
from utils import *
from sap_sqls import *
from ending_onhand import * 
import datetime
import threading
import time
import pandas as pd

class SAP_Master_Reset(Email_Utils):    
    def initial_history_table(self, table_name):
        self.connect_azuredb()
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        sql = f'''select count(1) from {table_name} where Batch_Date = '{current_date}' '''
        init_sql = f'''
                DELETE FROM {table_name}
                WHERE Batch_Date = '{current_date}' '''

        if int(self.fetch_data(sql).values[0][0]) > 1:
            print('[EVENT] {} HAS ALREADY SAVED TODAY. RPA IS INITIALIZING NOW'.format(table_name))
            self.cursor.execute(init_sql)
            self.conn.commit() 

    def atp_batch(self):
        print('[EVENT] STARTING TO SAVE TODAY HISTORY ATP RESULT')
        self.excel_name = 'ATP_BATCH_HISTORY'
        self.connect_azuredb()
        self.df_atp = self.fetch_data(sql_atp)
        self.df_atp = self.df_atp.fillna('')
        self.initial_history_table('ATP_HISTORY')
        self.insert_pd_tosql('ATP_HISTORY', self.df_atp)
        self.write_logs('ATP_HISTORY', 'PASS', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'HISTORY')

    def atp_raw_history_batch(self):
        # [call sql table, save sql table, check column names]
        self.view_names = [
                        ['ATP_OUTBOUND' , 'OUT_BOUND_HISTORY', ['Product_Name', 'WH_Name']]
                        , ['ATP_ONHAND', 'ON_HAND_HISTORY', ['Product_Name', 'WH_Name']]
                        , ['ATP_INBOUND_DOMESTIC', 'INBOUND_DOMESTIC_HISTORY', ['Product_Name', 'WH_Name']]
                        , ['ATP_INBOUND_OVERSEA', 'INBOUND_OVERSEA_HISTORY', ['Product_Name']]
                        , ['ATP_DELIVERY_STATUS', 'DELIVERY_STATUS_HISTORY', ['Product_Name', 'WH_Name']]
                    ]
        
        for name in self.view_names:
            self.excel_name = name[1]        
            print('[EVENT] STARTING TO SAVE TODAY HISTORY {} RESULT'.format(name[0]))
            self.connect_azuredb()
            self.df = self.fetch_data('select * from {}'.format(name[0]))
            self.data_null_check(name[0], name[2])
            self.initial_history_table(name[1])        
            self.insert_pd_tosql(name[1], self.df)
            self.write_logs(name[1], 'PASS', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'HISTORY')

    def data_null_check(self, table_name, join_check_column_list):
        join_check_column_list_upper = list(map(str.upper,join_check_column_list))
        nullcheck_cols = [col for col in self.df.columns if col.upper() in join_check_column_list_upper]
        nullcheck_total_result = []
        if len(nullcheck_cols) > 0:
            for ncols in nullcheck_cols:
                nullcheck_result = self.df[ncols].isnull().sum() + len(self.df[self.df[ncols] == ''])
                if nullcheck_result > 0:
                    nullcheck_total_result.append([ncols, nullcheck_result])
                print('[EVENT] {} BATCH VIEW, {} COLUMN NULL CHECKING RESULT IS : {}'.format(table_name, ncols, nullcheck_result))

        if len(nullcheck_total_result) > 0:
            for null_col in nullcheck_total_result:
                null_col = null_col[0]
                df_error = self.df[(self.df[null_col].isna()) | (self.df[null_col] == '')].reset_index(drop = True)
                df_error.to_csv(Global.root_path + '/data/dummy/joinError_{}.csv'.format(null_col), encoding='utf-8-sig',  index = None)
            nullcheck_csv_path = [Global.root_path + '/data/dummy/joinError_{}.csv'.format(i[0]) for i in nullcheck_total_result]

            nullcheck_total_result = ['{} : {} ROWS MISSED'.format(i[0], i[1]) for i in nullcheck_total_result]
            nullcheck_total_result = ' <br> '.join(nullcheck_total_result)
            print('[WARNING] THIS ATTACHMENT HAS AN NULL VALUES.')
            # df_error = pd.concat([self.df[self.df[i].isna()] for i in nullcheck_cols]).reset_index(drop = True)
            # df_error.to_csv('../data/dummy/joinerror.csv', encoding='utf-8-sig',  index = None)
            
            eu = Email_Utils()
            eu.send_email('[RPA WARNING] {} JOIN CHECKING NOT VERIFIED'.format(table_name)
                            ,'ERROR MESSAGE'
                            ,'JoinNullWarning: {} FILE, JOIN CHECKING RESULT : <br> {}'.format(table_name, nullcheck_total_result)
                            , attachment_path = nullcheck_csv_path
                            , warning = True
                            , excel_name = self.excel_name
                            , RnRs = ['SAP', 'DEV', 'PLAN']
                            )
            del eu

    def atp_ending_onhand_batch(self):
        print('[EVENT] STARTING TO SAVE TODAY HISTORY ATP ENDING ONHAND RESULT')
        self.excel_name = 'ATP_BATCH_HISTORY'
        eoh = Ending_On_Hand()
        df_ending_onhand_result_join = eoh.main()
        self.connect_azuredb()
        self.initial_history_table('ATP_BI_ENDING_ONHAND')
        self.insert_pd_tosql('ATP_BI_ENDING_ONHAND', df_ending_onhand_result_join)
        self.write_logs('ATP_BI_ENDING_ONHAND', 'PASS', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'HISTORY')
        del eoh
    
    def atp_data_check(self):
        eu = Email_Utils()
        eu.connect_azuredb()
        df = eu.fetch_data('select * from OSR_CPO_ETD_CHECK')
        if len(df) >= 1:
            print(df)
            # eu.send_email('[RPA WARNING] OUTBOUND CPO DELIVERED-DONE BEFORE ALLOCATION CW'
            #                 ,'ERROR MESSAGE'
            #                 ,'ValueWarning: DATE VALIDATION CHECKING RESULT, <br> OUTBOUND CPO DELIVERED-DONE BEFORE ALLOCATION CW' 
            #                 ,appendix = df.to_html(index=False).replace('<td>', '<td align="center">')
            #                 ,warning = True
            #                 ,excel_name = 'ORDER_STATUS_REPORT'
            #                 ,RnRs=['OSR', 'PLAN', 'DEV']
            #                 )
        eu.connect_azuredb()
        df = eu.fetch_data('select * from OSR_CPO_ATD_CHECK')
        if len(df) >= 1:
            print(df)

            # eu.send_email('[RPA WARNING] OUTBOUND NOT-DELIVERED AFTER ALLOCATION CW'
            #                 ,'ERROR MESSAGE'
            #                 ,'ValueWarning: DATE VALIDATION CHECKING RESULT, <br> OUTBOUND CPO NOT-DELIVERED AFTER ALLOCATION CW' 
            #                 ,appendix = df.to_html(index=False).replace('<td>', '<td align="center">')
            #                 ,warning = True
            #                 ,excel_name = 'ORDER_STATUS_REPORT'
            #                 ,RnRs=['OSR', 'PLAN','DEV', '3PL_OUTBOUND']
            #                 )
        eu.connect_azuredb()
        df = eu.fetch_data('select * from OSR_CPO_PCS_CHECK')
        if len(df) >= 1:
            print(df)

            # eu.send_email('[RPA WARNING] OUTBOUND PCS IS GREATER THEN OSR PCS'
            #                 ,'ERROR MESSAGE'
            #                 ,'ValueWarning: DATE VALIDATION CHECKING RESULT, <br> OUTBOUND PCS IS GREATER THEN OSR PCS' 
            #                 ,appendix = df.to_html(index=False).replace('<td>', '<td align="center">')
            #                 ,warning = True
            #                 ,excel_name = 'ORDER_STATUS_REPORT'
            #                 ,RnRs=['PLAN','DEV', '3PL_OUTBOUND', 'OSR']
            #                 )

        del eu

if __name__ == '__main__':
    smr = SAP_Master_Reset()
    # smr.read_qspdb()
    # smr.update_sap_data()
    # smr.atp_raw_history_batch()
    # smr.atp_batch()
    # smr.atp_ending_onhand_batch()
    smr.atp_data_check()
    #PR validation add!