sql = '''
with t1 as (
    select ISNULL(WH_Location, 'MISS')  AS WH_Location, Item_Code, SUM(ON_HAND) AS ON_HAND from (
    select WH_Location , Item_Code , SUM(PCS) AS ON_HAND from ON_HAND_SAP ohs 
    where Updated_Date = (select max(Updated_Date) from ON_HAND_SAP )
    group by WH_Location , Item_Code

    UNION 

    select distinct WH_Location, Item_Code ,0 AS ON_HAND from ATP_BI_HISTORY_SUMMARY2 abh
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
from ATP_BI_HISTORY_SUMMARY2 abh 
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