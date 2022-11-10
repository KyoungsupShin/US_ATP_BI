import sys
sys.path.append('../utils')
from db_tools import *
from batch_tools import * 
from email_tools import *

class Test_Utils(SAP_Master_Reset, Outlook_Utils, Warning_Utils):
    def __init__(self):
        self.email_test()
        self.db_connect_test()
        self.view_table_test()
        self.pbix_refresh_test()

    def db_connect_test(self):
        self.connect_qspdb()
        self.connect_azuredb()

    def email_test(self):
        ou = Outlook_Utils()
        del ou
        pass

    def pbix_refresh_test(self):
        print('[TEST] PBIX-Web is being tested by refresh.')
        self.pbix_api_refresh()

    def view_table_test(self):
        print('[TEST] View table is being tested by refresh.')
        view_table_list_sql = '''
            select TABLE_NAME  from INFORMATION_SCHEMA.tables
            where TABLE_TYPE ='VIEW'  
                AND TABLE_NAME Not like '%TEST%'
                AND (TABLE_NAME like 'OSR%' OR TABLE_NAME like 'ATP%')'''
        test_view_tables = self.fetch_data(view_table_list_sql)['TABLE_NAME'].tolist()
        for test_view_table in test_view_tables:
            try:
                test_sql = f'SELECT TOP 1 * FROM {test_view_table}'
                self.fetch_data(view_table_list_sql)
                print(test_view_table + ' SQL TEST PASSED.')
            except Exception as e:
                raise ConnectionError(test_view_table + ' SQL TEST FAILED.')
    
    
if __name__ == '__main__':
    tu = Test_Utils()