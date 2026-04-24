-- ============================================================================
-- Vista: Payments
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Extrae y transforma datos de pagos desde Firestore
-- ============================================================================

SELECT                                                                                                                                                     
    document_id, -- El ID del documento de Firestore                                                                                                         
    CAST(JSON_EXTRACT_SCALAR(data, '$.description') AS STRING) AS description,                                                                               
    -- Usamos SAFE.PARSE_TIMESTAMP para manejar errores en la marca de tiempo 'endDate'                                                                      
    SAFE.PARSE_TIMESTAMP('%d de %B de %Y, %I:%M:%S.%E3%p UTC-3', JSON_EXTRACT_SCALAR(data, '$.endDate'), 'America/Argentina/Buenos_Aires') AS end_date,      
    CAST(JSON_EXTRACT_SCALAR(data, '$.id') AS STRING) AS id,                                                                                                 
    CAST(JSON_EXTRACT_SCALAR(data, '$.isDeleted') AS BOOL) AS is_deleted,                                                                                    
    -- maxAmountPerUser es un string que puede ser numérico o vacío. Lo casteamos a NUMERIC de forma segura.                                                 
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.maxAmountPerUser') AS NUMERIC) AS max_amount_per_user,                                                            
    CAST(JSON_EXTRACT_SCALAR(data, '$.name') AS STRING) AS name,                                                                                             
    -- paymentMethods es un array, lo extraemos como ARRAY<STRING>                                                                                           
    CAST(JSON_EXTRACT_ARRAY(data, '$.paymentMethods') AS ARRAY<STRING>) AS payment_methods,                                                                  
    -- Usamos SAFE.PARSE_TIMESTAMP para manejar errores en la marca de tiempo 'startDate'                                                                    
    SAFE.PARSE_TIMESTAMP('%d de %B de %Y, %I:%M:%S.%E3%p UTC-3', JSON_EXTRACT_SCALAR(data, '$.startDate'), 'America/Argentina/Buenos_Aires') AS start_date,  
    CAST(JSON_EXTRACT_SCALAR(data, '$.typePayment') AS STRING) AS type_payment,                                                                              
    -- 'value' es un string que puede ser numérico. Lo casteamos a NUMERIC de forma segura.                                                                  
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.value') AS NUMERIC) AS value                                                                                      
  FROM                                                                                                                                                       
    `casla-fanhub-admin-prod.firestore_export.stream_raw_latest`                                                                                             
  WHERE                                                                                                                                                      
    document_name LIKE '%Payments%'
