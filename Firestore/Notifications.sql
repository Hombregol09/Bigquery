-- ============================================================================
-- Vista: Notifications
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Extrae y transforma datos de notificaciones desde Firestore
-- ============================================================================

SELECT                                                                                                                          
    document_id AS id,                                                                                                            
    CAST(JSON_EXTRACT_SCALAR(data, '$.adminUserId') AS STRING) AS adminUserId,                                                    
                                                                                                                                   
    -- CreationDate como TIMESTAMP y DATE                                                                                         
    TIMESTAMP_SECONDS(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.creationDate._seconds') AS INT64)) AS creationDate_timestamp,        
    DATE(TIMESTAMP_SECONDS(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.creationDate._seconds') AS INT64))) AS creationDate,            
                                                                                                                                   
    CAST(JSON_EXTRACT_SCALAR(data, '$.dni') AS STRING) AS dni,                                                                    
    CAST(JSON_EXTRACT_SCALAR(data, '$.isDeleted') AS BOOL) AS isDeleted,                                                          
    CAST(JSON_EXTRACT_SCALAR(data, '$.isRead') AS BOOL) AS isRead,                                                                
    CAST(JSON_EXTRACT_SCALAR(data, '$.memberId') AS STRING) AS memberId,                                                          
    CAST(JSON_EXTRACT_SCALAR(data, '$.message') AS STRING) AS message,                                                            
                                                                                                                                   
    -- PublicationDate como TIMESTAMP y DATE                                                                                      
    TIMESTAMP_SECONDS(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.publicationDate._seconds') AS INT64)) AS publicationDate_timestamp,  
    DATE(TIMESTAMP_SECONDS(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.publicationDate._seconds') AS INT64))) AS publicationDate,      
                                                                                                                                   
    CAST(JSON_EXTRACT_SCALAR(data, '$.title') AS STRING) AS title                                                                 
                                                                                                                                   
  FROM `casla-fanhub-admin-prod.firestore_export.stream_raw_latest`                                                               
  WHERE document_name LIKE 'projects/casla-fanhub-admin-prod/databases/(default)/documents/Notifications/%'
