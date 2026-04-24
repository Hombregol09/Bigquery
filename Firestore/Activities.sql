-- ============================================================================
-- Vista: Activities
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Extrae y transforma datos de actividades desde Firestore
-- ============================================================================

SELECT                                                                        
    document_id,                                                                
    CAST(JSON_EXTRACT_SCALAR(data, '$.activityType') AS STRING) AS activity_type,  
    CAST(JSON_EXTRACT_SCALAR(data, '$.description') AS STRING) AS description,    
    CAST(JSON_EXTRACT_SCALAR(data, '$.id') AS STRING) AS id,                    
    CAST(JSON_EXTRACT_SCALAR(data, '$.isDeleted') AS BOOL) AS is_deleted,       
    CAST(JSON_EXTRACT_SCALAR(data, '$.name') AS STRING) AS name,                
    CAST(JSON_EXTRACT_ARRAY(data, '$.places') AS ARRAY<STRING>) AS places       
  FROM                                                                          
    `casla-fanhub-admin-prod.firestore_export.stream_raw_latest`                  
  WHERE                                                                         
    document_name LIKE '%Activities%';
