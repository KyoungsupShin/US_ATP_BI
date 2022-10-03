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
            if name[0] == 'ATP_OUTBOUND':
                self.df = self.fetch_data('''SELECT * FROM ATP_OUTBOUND 
                                             WHERE Logi_Status not like 'Can%'
                                             OR CPO_Status NOT IN ('CANCELED', 'CONSUMED')  ''')
            else:      
                self.df = self.fetch_data('select * from {}'.format(name[0]))
            self.data_null_check(name[0], name[2])
            self.initial_history_table(name[1])        
            self.insert_pd_tosql(name[1], self.df)
            self.write_logs(name[1], 'PASS', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'HISTORY')

    def data_null_check(self, table_name, join_check_column_list):
        RnRs = ['SAP', 'DEV', 'PLAN']
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
                if table_name == 'ATP_OUTBOUND':
                    table_name = 'OSR & Outbound'
                    RnRs = ['3PL_OUTBOUND', 'DEV', 'PLAN']
                    df_error.rename(columns = {'WH_Code' : 'WH_Location'}, inplace = True)
                df_error.to_csv(Global.root_path + '/data/dummy/joinError_{}.csv'.format(null_col), encoding='utf-8-sig',  index = None)

            nullcheck_csv_path = [Global.root_path + '/data/dummy/joinError_{}.csv'.format(i[0]) for i in nullcheck_total_result]
            nullcheck_total_result = ['{} : {} ROWS MISSED'.format(i[0], i[1]) for i in nullcheck_total_result]
            nullcheck_total_result = ' <br> '.join(nullcheck_total_result)
            print('[WARNING] THIS ATTACHMENT HAS AN NULL VALUES.')
            
            eu = Email_Utils()
            eu.send_email('[ATP] {} item codes & warehouses not on SAP master'.format(table_name)
                            ,'ERROR MESSAGE'
                            ,f'''Impact: Total available inventory amount is not affected, but when filtering by item codes, product names, or warehouses, the available inventory may be decreased.
                             <br>Instructions: Please check the attached cases and <br> 
                             (1) add any blank item codes or WH_codes, or/and 
                             <br> (2) fix incorrect item codes or WH_codes, or/and 
                             <br> (3) if new item codes, add them to SAP
                             <br><br> {nullcheck_total_result} <br>'''
                            , attachment_path = nullcheck_csv_path
                            , warning = True
                            , excel_name = self.excel_name
                            # , destination= 'dany.shin@hanwha.com'
                            , RnRs = RnRs
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
        # df = eu.fetch_data('select * from OSR_CPO_ETD_CHECK')
        # if len(df) >= 1:
        #     eu.send_email('[ATP] OSR allocation CW in future but outbound already delivered'
        #                     ,'ERROR MESSAGE'
        #                     ,''' Impact: Both hard allocation and CPO shipped will be counted (Double counted), thus decreasing future available inventory amount.
        #                         <br>Instructions: Please update the OSR ALLOC_CW to match Outbount ATD.'''
        #                     ,appendix = df.to_html(index=False).replace('<td>', '<td align="center">')
        #                     ,warning = True
        #                     ,excel_name = 'ORDER_STATUS_REPORT'
        #                     ,RnRs=['OSR', 'PLAN', 'DEV', '3PL_OUTBOUND']
        #                     )

        eu.connect_azuredb()
        df = eu.fetch_data('select * from OSR_CPO_ATD_CHECK')
        if len(df) >= 1:
            eu.send_email('[ATP] OSR allocation CW in past, but outbound not delivered'
                            ,'ERROR MESSAGE'
                            ,f''' Impact: Hard-allocations will remain in the past so no impact on ATP (These will be moved to shipped once ETD is added)
                                <br> Instructions: There are {len(df)} POs that have passed the original requested delivery dates. 
                                <br> Please use this list for your reference.''' 
                            ,appendix = df.to_html(index=False).replace('<td>', '<td align="center">')
                            ,warning = True
                            ,excel_name = 'ORDER_STATUS_REPORT'
                            ,RnRs=['OSR', 'PLAN','DEV', '3PL_OUTBOUND']
                            )
        eu.connect_azuredb()
        df = eu.fetch_data('select * from OSR_CPO_PCS_CHECK')
        if len(df) >= 1:
            eu.send_email('[ATP] Outbound PCS is greater than OSR PCS'
                            ,'ERROR MESSAGE'
                            ,''' Impact: Shipped PCS will be greater than hard allocation PCS, thus decreasing future available inventory amount.
                                <br> Instructions: Please double check the below cases on both OSR & Outbound reports and fix the incorrect PCS count.''' 
                            ,appendix = df.to_html(index=False).replace('<td>', '<td align="center">')
                            ,warning = True
                            ,excel_name = 'ORDER_STATUS_REPORT'
                            ,RnRs=['PLAN','DEV', 'OSR', '3PL_OUTBOUND'])
        eu.connect_azuredb()
        df = eu.fetch_data('select * from ALLOCATION_NEW_ITEM_CHECK')        
        if len(df) >= 1:
            eu.send_email('[RPA WARNING] QBIS ALLOCATION PLAN NEW ITEMCODE CREATED'
                            ,'ERROR MESSAGE'
                            ,'ValueWarning: DATE VALIDATION CHECKING RESULT, <br> QBIS ALLOCATION PLAN NEW ITEMCODE CREATED' 
                            ,appendix = df.to_html(index=False).replace('<td>', '<td align="center">')
                            ,warning = True
                            ,excel_name = 'ORDER_STATUS_REPORT'
                            ,RnRs=['PLAN','DEV', '3PL_INBOUND'])

        eu.connect_azuredb() # added 9.7.22
        df = eu.fetch_data('select * from OSR_CPO_MATCH_CHECK')        
        if len(df) >= 1:
            eu.send_email('[CRITICAL][ATP] CPO# or item code discrepancies between Outbound and OSR'
                            ,'ERROR MESSAGE'
                            ,'''Error: (1) CPO# on Outbound not found on OSR, or/and 
                                <br> (2) CPO# on Outbound & OSR the same, but item codes different
                                <br>Impact: CPO on Outbound (Shipped) and potentially the same CPO on OSR (but in different CPO#) will both be counted  
                                <br> (Double counted), thus decreasing future available inventory amount.
                                <br> Instructions: Please review the below CPO#s and update to correct CPO# on OSR or Outbound as necessary, as well as correct item codes.''' 
                            ,appendix = df.to_html(index=False).replace('<td>', '<td align="center">')
                            ,warning = True
                            ,excel_name = 'ORDER_STATUS_REPORT'
                            ,RnRs=['PLAN','DEV', 'OSR', '3PL_OUTBOUND']
                            ,critical=True)

        del eu

if __name__ == '__main__':
    smr = SAP_Master_Reset()
    smr.read_qspdb()
    smr.update_sap_data()
    smr.atp_raw_history_batch()
    smr.atp_batch()
    smr.atp_ending_onhand_batch()
    # smr.atp_data_check()
    # PR validation add!