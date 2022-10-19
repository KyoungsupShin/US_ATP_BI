# sql_wh_code_sql = '''
# SELECT 
#   tc.CD_VAL3	AS 'WH_Location'
#   ,tc.CD_NM	AS 'WH_3PL_Name'
#   ,tc.CD_VAL3	AS 'SAP_WH_Location_Group'
#   ,t2.COMM_CD AS 'SAP_WH_Location_Code'
#   ,tc.CD_NM	AS 'SAP_3PL_WH_Name'
#   ,CASE WHEN tc.CD_VAL = 'N' THEN 'Y' ELSE 'N' END AS 'UseYN' 
#   FROM hanwha_qcells.dbo.TB_CMMNCODE tc 
#   LEFT JOIN (
#       SELECT 
#           COMM_CD	 
#           , CD_VAL3 
#       FROM hanwha_qcells.dbo.TB_CMMNCODE 
#       WHERE REPR_CD = 'US062'  and COMM_CD like 'A%') t2 
#       ON tc.CD_VAL3 = t2.CD_VAL3 COLLATE Korean_Wansung_CS_AS
#   WHERE REPR_CD = 'US062' AND  tc.CD_VAL3 IS NOT NULL 
#   AND (LEFT(tc.CD_NM COLLATE Korean_Wansung_CS_AS , 2) = LEFT(t2.CD_VAL3, 2)) and tc.CD_NM != tc.CD_VAL3
#   ORDER BY tc.SORT_SEQ ASC'''


sql_wh_code_sql = '''
    SELECT 
      tc.CD_VAL3 AS 'WH_Location',							
      tc.CD_NM AS 'WH_3PL_Name',							
      tc.CD_VAL3 AS 'SAP_WH_Location_Group',							
      tc.CD_VAL4 AS 'SAP_WH_Location_Code',							
      tc.CD_NM AS 'SAP_3PL_WH_Name',							
    --	tc.COMM_CD AS '(New) SAP 3PL W/H Name Code',							
    --	tc.CD_VAL5 AS '(New)DDP Virtual W/H',							
      CASE WHEN tc.CD_VAL = 'N' THEN 'Y' ELSE 'N' END AS 'UseYN' 							
    FROM hanwha_qcells.dbo.TB_CMMNCODE tc							
    WHERE REPR_CD = 'US062'							
      AND tc.CD_VAL4 IS NOT NULL							
    ORDER BY tc.CD_VAL4 ASC		
'''

sql_item_code_sql = '''
        SELECT
         TI1.ITEM_NO AS 'Item_Code'
        , ISNULL(TC2.CD_NM,'')                 AS 'ProductName'
        , ISNULL(TI1.POWER_CLS_CD,'')			AS 'Power_Class'        
        , ISNULL(LEFT(TI1.DETAIL_NAME, CHARINDEX('_', TI1.DETAIL_NAME, 1)-1),'') AS 'FactoryCd'
        , CASE 
            WHEN SUBSTRING(TI1.DETAIL_NAME, CHARINDEX('_', TI1.DETAIL_NAME, CHARINDEX('_', TI1.DETAIL_NAME, 0) + 1) + 1, 3) ='RES' THEN 'Residential'
            WHEN SUBSTRING(TI1.DETAIL_NAME, CHARINDEX('_', TI1.DETAIL_NAME, CHARINDEX('_', TI1.DETAIL_NAME, 0) + 1) + 1, 3) ='ULC' THEN 'U&C'
            WHEN SUBSTRING(TI1.DETAIL_NAME, CHARINDEX('_', TI1.DETAIL_NAME, CHARINDEX('_', TI1.DETAIL_NAME, 0) + 1) + 1, 3) ='C&I' THEN 'U&C' 
            ELSE '' END AS SegmentName
        , ISNULL(REPLACE(TI1.DETAIL_NAME, '_', ' / '),'')	AS 'ProductGroup3'
        , CASE WHEN TI1.ACTIVE_YN = 'Y' THEN 'Active' ELSE 'Inactive' END AS 'UseYN'
        FROM
          hanwha_qcells.dbo.TB_ITEM(NOLOCK) TI1
          /* WATT */
          LEFT OUTER JOIN
            hanwha_qcells.dbo.TB_CMMNCODE(NOLOCK) TC1
            ON
              TC1.REPR_CD                                  = 'US010'
              AND TC1.NAT_CD                               = 'US'
              AND TC1.USE_YN                               = 'Y'
              AND TC1.COMM_CD COLLATE Korean_Wansung_CS_AS = TI1.POWER_CLS_CD
          /* ITEM_TYPE */
          LEFT OUTER JOIN
            hanwha_qcells.dbo.TB_CMMNCODE(NOLOCK) TC2
            ON
              TC2.REPR_CD                                  = 'US011'
              AND TC2.NAT_CD                               = 'US'
              AND TC2.USE_YN                               = 'Y'
              AND TC2.COMM_CD COLLATE Korean_Wansung_CS_AS = TI1.PDT_TYPE_CD
          /* ITEM_GRP */
          LEFT OUTER JOIN
            hanwha_qcells.dbo.TB_CMMNCODE(NOLOCK) TC3
            ON
              TC3.REPR_CD                                  = 'US012'
              AND TC3.NAT_CD                               = 'US'
              AND TC3.USE_YN                               = 'Y'
              AND TC3.COMM_CD COLLATE Korean_Wansung_CS_AS = TI1.ITEM_GRP_CD
          /* DIV_CD */
          LEFT OUTER JOIN
            hanwha_qcells.dbo.TB_CMMNCODE(NOLOCK) TC4
            ON
              TC4.REPR_CD                                  = 'US043'
              AND TC4.NAT_CD                               = 'US'
              AND TC4.USE_YN                               = 'Y'
              AND TC4.COMM_CD COLLATE Korean_Wansung_CS_AS = TI1.DIV_CD
          /* TB_BOM */
          LEFT OUTER JOIN
            hanwha_qcells.dbo.TB_BOM (NOLOCK) TB
            ON
              TB.ITEM_SEQ = TI1.ITEM_SEQ
        WHERE
          TI1.NAT_CD                   = 'US'
          AND TI1.ACTIVE_YN            = 'Y'
          AND TI1.DEL_YN               = 'N'
          AND TI1.SALES_ITEM_YN        = 'Y'
          AND ISNULL(TB.BOM_TYPE, '') != 'P'
          AND ISNULL(TB.DEL_YN, '')   != 'Y'
'''

sql_atp = '''
    select ISNULL(Data_Type, '') AS Data_Type
        , ISNULL(기준일자, '') AS 기준일자
        , ISNULL(날짜, '') AS 날짜
        , ISNULL(YYYYMM, '') AS YYYYMM
        , ISNULL(CW, '') AS CW
        , ISNULL(제품명, '') AS 제품명
        , ISNULL(Item_Code, '') AS Item_Code
        , ISNULL(Power_Class, '') AS Power_Class
        , ISNULL(WH_Name, '') AS WH_Name
        , ISNULL(Tariff, '') AS Tariff
        , ISNULL(WestEast, '') AS WestEast
        , ISNULL(Intake_Shipments_FCST, '') AS Intake_Shipments_FCST
        , ISNULL(Intake_Shipments_ACTUAL, '') AS Intake_Shipments_ACTUAL
        , ISNULL(Soft_Allocation, '') AS Soft_Allocation
        , ISNULL(Hard_Allocation, '') AS Hard_Allocation
        , ISNULL(CPO_shipped, '') AS CPO_shipped
        , ISNULL(PO, '') AS PO
    from ATP_BI
'''


