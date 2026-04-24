-- ============================================================================
-- Vista: PagoticPayment_v2
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Versión optimizada con deduplicación liviana sobre stream_raw_changelog
-- ============================================================================

WITH BasePagos AS (
    SELECT 
      document_id,
      data
    FROM `casla-fanhub-admin-prod.firestore_export.stream_raw_changelog`
    WHERE document_name LIKE 'projects/casla-fanhub-admin-prod/databases/(default)/documents/PagoticPayments/%'
    -- 1. Descartamos los borrados que hayan ocurrido hoy
    AND operation != 'DELETE' 
    -- 2. Deduplicación liviana: nos quedamos solo con el último estado del día
    QUALIFY ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY timestamp DESC) = 1 
)
SELECT DISTINCT                                                                                                             
    p.document_id, -- ID del documento                                                                                        
    e.description AS event_description,                                                                                       
                                                                                                                               
    SAFE_CAST(JSON_EXTRACT_SCALAR(p.data, '$.amount') AS NUMERIC) AS amount,                                                  
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.buyerMemberId') AS STRING) AS buyerMemberId,                                          
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.concept_id') AS STRING) AS concept_id,                                                
                                                                                                                               
    -- created_at en formato YYYY/MM/DD                                                                                       
    FORMAT_DATE('%Y/%m/%d', DATE(                                                                                             
      IFNULL(                                                                                                                 
        COALESCE(                                                                                                             
          TIMESTAMP_SECONDS(SAFE_CAST(JSON_EXTRACT_SCALAR(p.data, '$.createdAt._seconds') AS INT64)),                         
          SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', JSON_EXTRACT_SCALAR(p.data, '$.createdAt')),                           
          SAFE.PARSE_TIMESTAMP('%d de %B de %Y, %I:%M:%S %p',                                                                 
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(  
              JSON_EXTRACT_SCALAR(p.data, '$.createdAt'),                                                                     
              ' UTC-3',''),'p.m.','PM'),'Enero','January'),'Febrero','February'),'Marzo','March'),                            
              'Abril','April'),'Mayo','May'),'Junio','June'),'Julio','July'),'Agosto','August'),                              
              'Septiembre','September'),'Octubre','October'),'Noviembre','November'),'Diciembre','December'),                 
            'America/Argentina/Buenos_Aires')                                                                                 
        ),                                                                                                                    
        TIMESTAMP('1970-01-01 00:00:00 UTC')                                                                                  
      )                                                                                                                       
    )) AS created_at_ddmmyyyy,                                                                                                
                                                                                                                               
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.description') AS STRING) AS description,                                              
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.dni') AS STRING) AS dni,                                                              
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.email') AS STRING) AS email,                                                          
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.event') AS STRING) AS event,                                                          
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.externalTransactionId') AS STRING) AS external_transaction_id,                        
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.external_reference') AS STRING) AS external_reference,                                
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.formUrl') AS STRING) AS form_url,                                                     
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.name') AS STRING) AS name,                                                            
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.pagoticId') AS STRING) AS pagotic_id,                                                 
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.status') AS STRING) AS status,                                                        
                                                                                                                               
    -- UNNEST de ticketIds                                                                                                    
    ticket_id,                                                                                                                
                                                                                                                               
    -- Nuevos campos en rojo por eze                                                                                          
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.paymentPlanId') AS STRING) AS payment_plan_id,                                        
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.sectorId') AS STRING) AS sector_id,                                                   
    SAFE_CAST(JSON_EXTRACT_SCALAR(p.data, '$.ticketsCount') AS INT64) AS tickets_count,                                       
                                                                                                                               
    -- Campos dentro de paymentMethods                                                                                        
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.paymentMethods.installments') AS STRING) AS installments,                             
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.paymentMethods.installment_amount') AS STRING) AS installment_amount,                 
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.paymentType') AS STRING) AS payment_type,                              
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.brand') AS STRING) AS brand,                                           
                                                                                                                               
    -- updated_at en formato YYYY/MM/DD                                                                                       
    FORMAT_DATE('%Y/%m/%d', DATE(                                                                                             
      IFNULL(                                                                                                                 
        COALESCE(                                                                                                             
          TIMESTAMP_SECONDS(SAFE_CAST(JSON_EXTRACT_SCALAR(p.data, '$.updatedAt._seconds') AS INT64)),                         
          SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', JSON_EXTRACT_SCALAR(p.data, '$.updatedAt')),                           
          SAFE.PARSE_TIMESTAMP('%d de %B de %Y, %I:%M:%S %p',                                                                 
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(  
              JSON_EXTRACT_SCALAR(p.data, '$.updatedAt'),                                                                     
              ' UTC-3',''),'p.m.','PM'),'Enero','January'),'Febrero','February'),'Marzo','March'),                            
              'Abril','April'),'Mayo','May'),'Junio','June'),'Julio','July'),'Agosto','August'),                              
              'Septiembre','September'),'Octubre','October'),'Noviembre','November'),'Diciembre','December'),                 
            'America/Argentina/Buenos_Aires')                                                                                 
        ),                                                                                                                    
        TIMESTAMP('1970-01-01 00:00:00 UTC')                                                                                  
      )                                                                                                                       
    )) AS updated_at_ddmmyyyy                                                                                                 
                                                                                                                               
  FROM                                                                                                                        
    BasePagos AS p,                                                                                       
    UNNEST(JSON_EXTRACT_ARRAY(p.data, '$.ticketIds')) AS ticket_id                                                            
                                                                                                                               
  LEFT JOIN                                                                                                                   
    `casla-fanhub-admin-prod.firestore_export.Events` AS e                                                                    
  ON                                                                                                                          
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.event') AS STRING) = e.event_id                                                       
                                                                                                                               
  WHERE                                                                                                                       
    ticket_id IS NOT NULL;
