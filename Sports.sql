-- ============================================================================
-- Vista: Sports
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Extrae y transforma datos de deportes desde Firestore
-- ============================================================================

SELECT                                                                          
    document_id, -- El ID del documento de Firestore                              
    CAST(JSON_EXTRACT_SCALAR(data, '$.adminUserId') AS STRING) AS admin_user_id,  
    -- 'coverPhoto' se omite según tu indicación                                  
    CAST(JSON_EXTRACT_SCALAR(data, '$.id') AS STRING) AS id,                      
    CAST(JSON_EXTRACT_SCALAR(data, '$.isDeleted') AS BOOL) AS is_deleted,         
    CAST(JSON_EXTRACT_SCALAR(data, '$.name') AS STRING) AS name                   
  FROM                                                                            
    `casla-fanhub-admin-prod.firestore_export.stream_raw_latest`                  
  WHERE                                                                           
    document_name LIKE '%/Sports/%' -- Filtro exacto para la colección 'Sports'
