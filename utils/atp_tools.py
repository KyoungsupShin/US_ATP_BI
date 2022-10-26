import sys
sys.path.append('../src')
from onhand_sqls import *
from email_tools import *
import warnings
warnings.filterwarnings("ignore")

class Ending_On_Hand(Outlook_Utils): #Overall Process: get atp data -> iteration key values(item_Code, wh_location, date) -> processing -> insert data
    def get_atp_data(self): #read atp data
        self.df_onhand = self.fetch_data(sql)
        self.df_atp = self.fetch_data(sql2)
        self.df_itemcode = self.fetch_data(sql3)
        self.df_wh = self.fetch_data(sql4)
        self.df_tariff = self.fetch_data(sql5)
    
    def get_ending_onhand(self, item_code_onhand): #filter reconcilation data, in-out data
        df1 = self.df_atp[self.df_atp['Item_Code'] == item_code_onhand]
        df2 = self.df_onhand[self.df_onhand['Item_Code']== item_code_onhand]
        df_union = pd.concat([df1, df2])
        df_union = df_union.groupby(['Dates','WH_Location', 'Item_Code']).agg({
                                                                            'ON_HAND' : 'sum', 
                                                                            'Intake' : 'sum', 
                                                                            'Soft_Allocation' : 'sum',
                                                                            'Hard_Allocation' : 'sum', 
                                                                            'CPO_shipped' : 'sum'}).reset_index()
        return df_union

    def calculate_ending_onhand(self, df_union):
        df_finals = pd.DataFrame()
        for wl in df_union['WH_Location'].unique():
            df_union_tmp = df_union[df_union['WH_Location'] == wl]
            df_union_tmp['SCM_FLOW'] = df_union_tmp['Intake'] + df_union_tmp['Soft_Allocation'] + df_union_tmp['Hard_Allocation'] + df_union_tmp['CPO_shipped']
            df_union_tmp['SCM_FLOW'] = df_union_tmp['SCM_FLOW'].cumsum(axis = 0)
            df_union_tmp['ON_HAND'] = df_union_tmp['ON_HAND'] + df_union_tmp['SCM_FLOW']
            df_final = pd.pivot_table(df_union_tmp,
                                        columns = 'Dates',
                                        values = ['Intake', 'Soft_Allocation', 'Hard_Allocation', 'CPO_shipped', 'ON_HAND'],
                                        aggfunc = 'sum')
            df_final['WH_Location'] =wl
            df_final['Item_Code'] = self.ic
            df_final = df_final.reset_index()
            df_final = df_final.set_index(['index', 'WH_Location', 'Item_Code'])    
            df_finals = df_finals.append(df_final)
        return df_finals

    def reshape_ending_onhand(self, df_finals):
        col_aligned = sorted(df_finals.columns)
        df_finals = df_finals[col_aligned]
        df_finals = df_finals.reset_index()
        df_finals[df_finals.columns[3]] = df_finals[df_finals.columns[3]].fillna(0)
        if df_finals.isnull().sum().sum() >= 1:
            df_finals[df_finals['index'] == 'ON_HAND'] = df_finals[df_finals['index'] == 'ON_HAND'].fillna(method= 'ffill', axis = 1)
        df_finals.fillna(0, inplace = True)
        df_finals = pd.melt(df_finals, id_vars = ['index','WH_Location', 'Item_Code'], value_vars = df_finals.columns[3:].tolist())
        df_finals.columns = ['index', 'WH_Location', 'Item_Code', 'Dates', 'value']
        df_finals = pd.pivot(df_finals, index = ['WH_Location', 'Item_Code', 'Dates'], columns = 'index', values = 'value')
        return df_finals

    def date_label(self,x):
        x = datetime.strptime(x, '%Y-%m-%d') + timedelta(days = 0)
        today = datetime.strptime(datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d') + timedelta(days = 0)
        day_delta = (x - today).days
        cw_delta = x.isocalendar()[1] - today.isocalendar()[1] 
        year_delta = x.year - today.year

        if cw_delta < 0:
            if year_delta >= 1:
                return 'Future Week'
            else:
                if cw_delta == -1:
                    return 'Previous Week'
                else:
                    return 'Reconsil Period'
        elif cw_delta == 0:
            if year_delta >= 1:
                return 'Future Week'
            elif day_delta == 0:
                return 'Current Week(Today)'
            else:
                return 'Current Week'
        elif (cw_delta < 9) & (cw_delta >= 1):
            if year_delta >= 1:
                return 'Future Week'
            else:
                return 'Future Week(Confidence)'
        elif cw_delta >= 9:
            return 'Future Week'
        else:
            return 'Reconsil Period'
       
    def join_ending_onhand(self, df_ending_onhand_result):
        df_ending_onhand_result = pd.merge(df_ending_onhand_result.reset_index(), self.df_itemcode, how = 'left' , on = 'Item_Code').reset_index(drop = True)
        df_ending_onhand_result = pd.merge(df_ending_onhand_result, self.df_wh, how = 'left' , on = 'WH_Location').reset_index(drop = True)
        df_ending_onhand_result['Tariff'] = df_ending_onhand_result['Item_Code'].apply(lambda x:self.get_tariff_rate(x))
        df_ending_onhand_result['CW'] = df_ending_onhand_result['Dates'].apply(lambda x:self.get_cw(x))
        df_ending_onhand_result['Dates'] = df_ending_onhand_result['Dates'].apply(lambda x:self.get_dates(x))
        df_ending_onhand_result = df_ending_onhand_result.fillna('')
        df_ending_onhand_result['CW_Type'] = df_ending_onhand_result['Dates'].apply(lambda x:self.date_label(x))
        return df_ending_onhand_result
    
    def Ending_onhand_main(self):
        def iso_yyyy(x, cw, rn):
            if rn >= 2:
                if int(cw[-2:]) > 25:
                    return df_ending_onhand_result_join[df_ending_onhand_result_join['RANK_YYYY']==2].YYYY_BI.min()
                else:
                    return df_ending_onhand_result_join[df_ending_onhand_result_join['RANK_YYYY']==2].YYYY_BI.max()
            else:
                return x
                
        self.get_atp_data()
        df_ending_onhand_result = pd.DataFrame()
        for self.ic in pd.concat([self.df_atp['Item_Code'], self.df_onhand['Item_Code']]).unique():        
            df_union = self.get_ending_onhand(self.ic)
            df_finals = self.calculate_ending_onhand(df_union)
            df_finals = self.reshape_ending_onhand(df_finals)
            df_ending_onhand_result = df_ending_onhand_result.append(df_finals)
        df_ending_onhand_result_join = self.join_ending_onhand(df_ending_onhand_result)
        df_ending_onhand_result_join['YYYY_BI'] = df_ending_onhand_result_join['Dates'].apply(lambda x:self.get_yyyy(x))
        df_ending_onhand_result_join['RANK_YYYY'] = df_ending_onhand_result_join.groupby(['CW'])['YYYY_BI'].transform('nunique')
        df_ending_onhand_result_join['YYYY_BI'] = df_ending_onhand_result_join.apply(lambda x: iso_yyyy(x['YYYY_BI'],x['CW'], x['RANK_YYYY']), axis = 1)
        col_db = ['WH_Location', 'Item_Code', 'ProductName', 'Power_Class', 'Tariff', 'WestEast', 
                'Dates', 'CPO_shipped', 'Hard_Allocation', 'Intake', 'ON_HAND', 'Soft_Allocation', 'CW_Type', 'SegmentName', 'YYYY_BI']
        return df_ending_onhand_result_join[col_db]

if __name__ == '__main__':
    eoh = Ending_On_Hand()
    df_ending_onhand_result_join = eoh.Ending_onhand_main()
    # self.insert_pd_tosql('ATP_BI_ENDING_ONHAND', df_ending_onhand_result_join)
