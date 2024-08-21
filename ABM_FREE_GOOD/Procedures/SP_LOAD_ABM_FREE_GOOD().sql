CREATE OR REPLACE PROCEDURE CDR.ABM_FREE_GOOD.SP_LOAD_ABM_FREE_GOOD()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '

/*
-- =============================================================================================================================
--   Author           	:      Sumit Sohale
--   Create date       	:      09/12/2022
--   Description       	:           
--   Change history    	:
--	 Updated by :	
-- 	 Update date		:
--   CR Information 	:    
--   Source Schema name :      ABM_FREE_GOOD_STG
--   Target Schema name :      ABM_FREE_GOOD
--	 Refresh Logic		:	   
-- =============================================================================================================================
*/

declare provider_feed_abbr := ''FREE_GOODS'';
		provider_feed_id varchar;
        file_id varchar;
        

begin

/************************************************************************* FREE_GOOD_ABM *****************************************************************/ 
/******************************************************************************************************************************************************************/


SELECT provider_feed_id  into :provider_feed_id from MPM.PROVIDER_FEED WHERE provider_feed_abbr=:provider_feed_abbr;
let table_nm1 := ''ABM_FREE_GOOD.FREE_GOOD_ABM'';
SELECT file_id into :file_id from MPM.FILE_DTL where stg_table_nm = :table_nm1;

            INSERT INTO MPM.FEED_INSTC 
            SELECT 
            A.PROVIDER_FEED_ID  AS PROVIDER_FEED_ID, 
            0                   AS FEED_STAT, 
            CURRENT_TIMESTAMP() AS START_DT, 
            NULL                AS END_DATE, 
            NULL                AS MIN_PD, 
            NULL                AS MAX_PD, 
            0                   AS REC_CNT, 
            CURRENT_TIMESTAMP() AS LOAD_DT, 
            NULL                AS ERR_DESC, 
            FILE_ID             AS FILE_ID 
            FROM  MPM.PROVIDER_FEED A LEFT JOIN MPM.FILE_DTL B ON stg_table_nm=:table_nm1 WHERE PROVIDER_FEED_ABBR=:provider_feed_abbr;

-------------------------------------*/Truncating existing data from the target table*/------------------------------

            DELETE FROM ABM_FREE_GOOD.FREE_GOOD_ABM WHERE ORDER_DATE >= (SELECT MIN(TO_DATE(ORDER_DATE)) FROM ABM_FREE_GOOD_STG.LND_FREE_GOOD_ABM); 
			
--------------------------------------/*Inserting data into the target table from the source table*/----------------------------------------------------------


			INSERT INTO ABM_FREE_GOOD.FREE_GOOD_ABM(REGION, ECOSYSTEM, TERRITORY, ORDER_NUMBER, ORDER_DATE, AOC_STATUS, PHARMACIST_MDM_ID,
			PHARMACIST_NAME, HOSPITAL_MDM_ID, HOSPITAL_NAME, SHIPPING_ADDRESS, CITY, STATE, ZIP, PRODUCT, 
			QUANTITY_ORDERED, QUANTITY_SHIPPED, SHIP_DATE)
            
					SELECT
					RAW.REGION,
					RAW.ECOSYSTEM,
					RAW.TERRITORY,
					RAW.ORDER_NUMBER,
					RAW.ORDER_DATE,
					RAW.AOC_STATUS,
					RAW.PHARMACIST_MDM_ID,
					RAW.PHARMACIST_NAME,
					RAW.HOSPITAL_MDM_ID,
					RAW.HOSPITAL_NAME,
					RAW.SHIPPING_ADDRESS,
					RAW.CITY,
					RAW.STATE,
					RAW.ZIP,
					RAW.PRODUCT,
					RAW.QUANTITY_ORDERED,
					RAW.QUANTITY_SHIPPED,
					RAW.SHIP_DATE
					FROM (
					SELECT
					RAW.REGION,
					RAW.ECOSYSTEM,
					RAW.TERRITORY,
					RAW.ORDER_NUMBER,
					RAW.ORDER_DATE,
					RAW.AOC_STATUS,
					RAW.PHARMACIST_MDM_ID,
					RAW.PHARMACIST_NAME,
					RAW.HOSPITAL_MDM_ID,
					RAW.HOSPITAL_NAME,
					RAW.SHIPPING_ADDRESS,
					RAW.CITY,
					RAW.STATE,
					RAW.ZIP,
					RAW.PRODUCT,
					RAW.QUANTITY_ORDERED,
					RAW.QUANTITY_SHIPPED,
					RAW.SHIP_DATE,
					MAX(MIN_MAX_BATCHES.MAX_ORDER_DATE) AS MAX_ORDER_DATE,
					MIN(MIN_MAX_BATCHES.MIN_ORDER_DATE) AS MIN_ORDER_DATE
					FROM (
						SELECT 
						REGION,
					ECOSYSTEM,
					TERRITORY,
					REPLACE(ORDER_NUMBER,''.0'','''') AS ORDER_NUMBER,
					TO_DATE(ORDER_DATE,''YYYY-MM-DD'') AS ORDER_DATE,
					CASE WHEN (AOC_STATUS = ''Open'' OR AOC_STATUS = '' '') THEN '' ''
						WHEN (AOC_STATUS = ''Closed'' OR AOC_STATUS = ''Complete'') THEN ''Complete'' END AS AOC_STATUS,
					PHARMACIST_MDM_ID,
					CONCAT(CONCAT(RTRIM(PHARMACIST_FIRST_NAME), '' ''), COALESCE(RTRIM(PHARMACIST_LAST_NAME),''''))  AS PHARMACIST_NAME,
					HOSPITAL_MDM_ID,
					HOSPITAL_NAME,
					SHIPPING_ADDRESS,
					CITY,
					STATE,
					CASE WHEN ZIP NOT LIKE ''%.%'' THEN ZIP ELSE SUBSTR(ZIP,1,REGEXP_INSTR(ZIP,''.'')-1) END AS ZIP,
					PRODUCT,
					CAST(QUANTITY_ORDERED AS DECIMAL(10,0)) AS QUANTITY_ORDERED,
					CAST(QUANTITY_SHIPPED AS DECIMAL(10,0)) AS QUANTITY_SHIPPED,
					TO_DATE(SHIP_DATE,''YYYY-MM-DD'') AS SHIP_DATE,
						BATCH_ID,
						ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS ID
						FROM (SELECT REGION, ECOSYSTEM, TERRITORY, ORDER_NUMBER, ORDER_DATE, AOC_STATUS, PHARMACIST_MDM_ID, 
					PHARMACIST_LAST_NAME, PHARMACIST_FIRST_NAME, HOSPITAL_MDM_ID, HOSPITAL_NAME, SHIPPING_ADDRESS, CITY, STATE, ZIP, 
					PRODUCT, QUANTITY_ORDERED, QUANTITY_SHIPPED, SHIP_DATE, TO_CHAR(CURRENT_TIMESTAMP, ''YYYY-MM-DD-hh-mm-ss'') AS BATCH_ID
					FROM ABM_FREE_GOOD_STG.LND_FREE_GOOD_ABM) 
					) RAW
					LEFT JOIN (
						SELECT 
						MAX(TO_DATE(RAW.ORDER_DATE,''YYYY-MM-DD'')) AS MAX_ORDER_DATE,
						MIN(TO_DATE(RAW.ORDER_DATE,''YYYY-MM-DD'')) AS MIN_ORDER_DATE,
						RAW.BATCH_ID
						FROM (SELECT REGION, ECOSYSTEM, TERRITORY, ORDER_NUMBER, ORDER_DATE, AOC_STATUS, PHARMACIST_MDM_ID, 
					PHARMACIST_LAST_NAME, PHARMACIST_FIRST_NAME, HOSPITAL_MDM_ID, HOSPITAL_NAME, SHIPPING_ADDRESS, CITY, STATE, ZIP, 
					PRODUCT, QUANTITY_ORDERED, QUANTITY_SHIPPED, SHIP_DATE, TO_CHAR(CURRENT_TIMESTAMP, ''YYYY-MM-DD-hh-mm-ss'') AS BATCH_ID
					FROM ABM_FREE_GOOD_STG.LND_FREE_GOOD_ABM) RAW
						GROUP BY BATCH_ID
					) MIN_MAX_BATCHES ON RAW.BATCH_ID < MIN_MAX_BATCHES.BATCH_ID AND RAW.ORDER_DATE BETWEEN MIN_MAX_BATCHES.MIN_ORDER_DATE AND MIN_MAX_BATCHES.MAX_ORDER_DATE
					GROUP BY 
						RAW.BATCH_ID,
						RAW.ID,
						RAW.REGION,
					RAW.ECOSYSTEM,
					RAW.TERRITORY,
					RAW.ORDER_NUMBER,
					RAW.ORDER_DATE,
					RAW.AOC_STATUS,
					RAW.PHARMACIST_MDM_ID,
					RAW.PHARMACIST_NAME,
					RAW.HOSPITAL_MDM_ID,
					RAW.HOSPITAL_NAME,
					RAW.SHIPPING_ADDRESS,
					RAW.CITY,
					RAW.STATE,
					RAW.ZIP,
					RAW.PRODUCT,
					RAW.QUANTITY_ORDERED,
					RAW.QUANTITY_SHIPPED,
					RAW.SHIP_DATE
					) RAW
					WHERE RAW.ORDER_DATE > RAW.MAX_ORDER_DATE OR RAW.ORDER_DATE < RAW.MIN_ORDER_DATE
					OR RAW.MAX_ORDER_DATE IS NULL OR RAW.MIN_ORDER_DATE IS NULL;

UPDATE MPM.FEED_INSTC T1 
            SET 
            FEED_STAT = 1, 
            END_DT    = CURRENT_TIMESTAMP(), 
			MIN_PD    = (SELECT MIN(ORDER_DATE) FROM identifier(:table_nm1) ), 
            MAX_PD    = (SELECT MAX(ORDER_DATE) FROM identifier(:table_nm1) ),
            REC_CNT   = (SELECT COUNT(*) FROM identifier(:table_nm1) ), 
            LOAD_DT   = CURRENT_TIMESTAMP() 
            WHERE T1.PROVIDER_FEED_ID = :provider_feed_id 
			AND T1.FILE_ID = :file_id
            AND START_DT = (SELECT MAX(START_DT) FROM MPM.FEED_INSTC WHERE PROVIDER_FEED_ID =:provider_feed_id AND FILE_ID = :file_id); 

	  return ''SP_LOAD_ABM_FREE_GOOD completed successfully'';
            
     exception
     
        when other then
        let line :=  sqlerrm;
        UPDATE MPM.FEED_INSTC T1 
        SET FEED_STAT = -1,
        END_DT        = CURRENT_TIMESTAMP(),
        REC_CNT       = 0, 
        LOAD_DT       = CURRENT_TIMESTAMP(),
        ERR_DESC      = :line
        WHERE T1.PROVIDER_FEED_ID = :provider_feed_id AND T1.FILE_ID = :file_id
        AND START_DT = (SELECT MAX(START_DT) FROM MPM.FEED_INSTC WHERE PROVIDER_FEED_ID = :provider_feed_id AND FILE_ID = :file_id);
         return line;  

		end;	
		 
		  ';