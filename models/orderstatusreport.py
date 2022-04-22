from msilib.schema import Error
import sys
sys.path.append('../utils')
from utils import * 

def ORDER_STATUS_REPORT_ETL():
    try:
        eu = ETL_Utils(excel_name = 'ORDER_STATUS_REPORT')
        eu.meta_sheet_info()
        eu.read_excel()
        eu.clean_data_text(eu.data_text)
        eu.clean_data_int(eu.data_int)
        eu.clean_data_int(int)
        eu.clean_data_float(eu.data_float)
        eu.clean_data_float(float)
        eu.clean_data_datetime(eu.yyyymmdd_datetime)
        
    except Exception as e:
        print('[ERROR] DURING ORDER_STATUS_REPORT FILE ERROR OCCURED : ', e)
        email_rpa = Email_Utils(mail_receivers = "digital_scm@us.q-cells.com")
        sql = f'''select MailAddr from ADMIN_INFO 
                WHERE RnR = 'Dev' '''
        email_rpa.connect_azuredb()
        email_rpa.checkisadmin = email_rpa.fetch_data(sql)
        email_rpa.conn.close()

        for mailaddr in email_rpa.checkisadmin['MailAddr'].tolist():
            email_rpa.send_email('[ERROR] ORDER_STATUS_REPORT FILE ETL STEP' 
                            ,'NEED TO CHECK OUT ERROR Message is : {}'.format(e) 
                            , destination =  mailaddr 
                            , master = False)
            break

if __name__ == '__main__':
    ORDER_STATUS_REPORT_ETL()