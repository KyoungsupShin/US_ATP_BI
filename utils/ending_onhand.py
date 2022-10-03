import sys
from utils import *
import warnings
warnings.filterwarnings("ignore")

sql = '''
with t1 as (
    select ISNULL(WH_Location, 'MISS')  AS WH_Location, Item_Code, SUM(ON_HAND) AS ON_HAND from (
    select WH_Location , Item_Code , SUM(PCS) AS ON_HAND from ON_HAND_SAP ohs 
    where Updated_Date = (select max(Updated_Date) from ON_HAND_SAP )
    group by WH_Location , Item_Code

    UNION 

    select distinct WH_Location, Item_Code ,0 AS ON_HAND from ATP_BI_HISTORY_SUMMARY abh
    where Item_Code is not NULL 
    ) t1
    group by WH_Location, Item_Code
)
select 
	CONVERT(Date, 날짜Key) AS Dates
	,t1.*
	, 0  as Intake 
	, 0 as Soft_Allocation 
	, 0 as Hard_Allocation 
	, 0 as CPO_shipped 
from t1, BI_CALENDAR bc 
WHERE 날짜Key between (
						select MAX(Based_Date) from ON_HAND_SAP ohs 
						where Updated_Date = (select max(Updated_Date) from ON_HAND_SAP )) 
					and DATEADD(dd, 240, getdate())
'''

sql2 = '''
select 날짜 as Dates
	, ISNULL(WH_Location, 'MISS') AS WH_Location 
	, Item_Code 
	, 0 AS ON_HAND
	, Intake_Shipments_FCST + Intake_Shipments_ACTUAL  as Intake 
	, Soft_Allocation * -1 AS Soft_Allocation
	, Hard_Allocation * -1 AS Hard_Allocation
	, CPO_shipped * -1 AS CPO_shipped 
from ATP_BI_HISTORY_SUMMARY abh 
WHERE 날짜 between (
						select MAX(Based_Date) from ON_HAND_SAP ohs 
						where Updated_Date = (select max(Updated_Date) from ON_HAND_SAP ))
				and DATEADD(dd, 240, getdate())
'''

sql3 = '''
    SELECT 
        CONCAT(t1.Item_Code, tc.Code) AS Item_Code
        , t1.ProductName
        , t1.Power_Class
        , t1.SegmentName
        , tc.Rate AS Tariff FROM (
        select 
            Distinct DummyID AS Item_Code
            , SUBSTRING(Product_Name, 
                        CHARINDEX( '/', Product_Name ,0) + 2, 
                        CHARINDEX( '/', Product_Name , CHARINDEX( '/', Product_Name ,0) + 2) - (CHARINDEX( '/', Product_Name ,0) + 3)) 
                        AS ProductName 
            , Power_Class
            , CASE WHEN Segment = 'ULC' THEN 'U&C' WHEN Segment = 'RES' THEN 'Residential' END AS SegmentName
        from POWER_CLASS_DIST aap 
    ) as t1 , TARIFF_CODE tc 
    UNION ALL
    select icms.Item_Code , icms.ProductName, icms.Power_Class, SegmentName , ISNULL(tc2.Rate,0) AS Tariff  from ITEM_CODE_MASTER_SAP_TARIFF icms
	left join TARIFF_CODE tc2 on RIGHT(icms.Item_Code,1) = tc2.Code  
'''

sql4 = '''
    select 
            distinct WH_Location 
            , WestEast
    from WAREHOUSE_INFO wi with(nolock)
    LEFT JOIN US_STATES_CODE usc 
    ON LEFT(wi.WH_Location,2) = LEFT(usc.StatesCode,2)
'''

sql5 = '''
    select Code, Rate AS Tariff 
    from TARIFF_CODE tc 
'''

class Ending_On_Hand(Email_Utils):
    def get_atp_data(self):
        self.connect_azuredb()
        self.df_onhand = self.fetch_data(sql)
        self.df_atp = self.fetch_data(sql2)
        self.df_itemcode = self.fetch_data(sql3)
        self.df_wh = self.fetch_data(sql4)
        self.df_tariff = self.fetch_data(sql5)
    
    def get_ending_onhand(self, item_code_onhand):
        df1 = self.df_atp[self.df_atp['Item_Code'] == item_code_onhand]
        df2 = self.df_onhand[self.df_onhand['Item_Code']== item_code_onhand]

        df_union = pd.concat([df1, df2])
        df_union = df_union.groupby(['Dates','WH_Location', 'Item_Code']).agg({
                                                                         'ON_HAND' : 'sum', 
                                                                        'Intake' : 'sum', 
                                                                      'Soft_Allocation' : 'sum',
                                                                      'Hard_Allocation' : 'sum', 
                                                                      'CPO_shipped' : 'sum'
                                                                     }).reset_index()
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
                             values = ['Intake', 'Soft_Allocation', 'Hard_Allocation', 'CPO_shipped', 'ON_HAND'],     # 데이터로 사용할 열
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
        df_finals[df_finals['index'] == 'ON_HAND'] = df_finals[df_finals['index'] == 'ON_HAND'].fillna(method= 'ffill', axis = 1)
        df_finals.fillna(0, inplace = True)
        df_finals = pd.melt(df_finals, id_vars = ['index','WH_Location', 'Item_Code'],
                            value_vars = df_finals.columns[3:].tolist())
        df_finals.columns = ['index', 'WH_Location', 'Item_Code', 'Dates', 'value']
        df_finals = pd.pivot(df_finals, index = ['WH_Location', 'Item_Code', 'Dates'], columns = 'index', values = 'value')
        return df_finals

    def get_cw(self, x):
        return "CW"+"{:02d}".format((x + timedelta(days = 0)).isocalendar()[1])

    def get_yyyymm(self,x):
        return str(x.year) + "{:02d}".format(int(x.month))

    def get_yyyymmdd(self,x):
        return str(x.year) + "{:02d}".format(int(x.month)) + "{:02d}".format(int(x.day))

    def get_dates(self,x):
        return datetime.strftime(x, '%Y-%m-%d')

    def get_tariff_rate(self, x):
        if len(str(x)) >= 1:
            rate = self.df_tariff[self.df_tariff['Code'] == str(x)[-1]]['Tariff']
            if len(rate) == 0:
                return 0
            else:
                return float(rate.values[0])

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
    def get_yyyy(self, x):
        return str(x[:4])

            
    def join_ending_onhand(self, df_ending_onhand_result):
        df_ending_onhand_result = pd.merge(df_ending_onhand_result.reset_index(), self.df_itemcode, how = 'left' , on = 'Item_Code').reset_index(drop = True)
        df_ending_onhand_result = pd.merge(df_ending_onhand_result, self.df_wh, how = 'left' , on = 'WH_Location').reset_index(drop = True)
        df_ending_onhand_result['Tariff'] = df_ending_onhand_result['Item_Code'].apply(lambda x:self.get_tariff_rate(x))
        df_ending_onhand_result['CW'] = df_ending_onhand_result['Dates'].apply(lambda x:self.get_cw(x))
        df_ending_onhand_result['Dates'] = df_ending_onhand_result['Dates'].apply(lambda x:self.get_dates(x))
        df_ending_onhand_result = df_ending_onhand_result.fillna('')
        df_ending_onhand_result['CW_Type'] = df_ending_onhand_result['Dates'].apply(lambda x:self.date_label(x))
        return df_ending_onhand_result
    
    def main(self):
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
    df_ending_onhand_result_join = eoh.main()
    df_ending_onhand_result_join.to_csv('final.csv')
    # self.insert_pd_tosql('ATP_BI_ENDING_ONHAND', df_ending_onhand_result_join)
