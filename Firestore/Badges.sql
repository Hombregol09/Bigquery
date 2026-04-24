-- ============================================================================
-- Vista: Badges
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Extrae y transforma datos de insignias desde Firestore
-- ============================================================================

SELECT
    document_id,                                                                            
    CAST(JSON_EXTRACT_SCALAR(data, '$.description') AS STRING) AS description,              
    CAST(JSON_EXTRACT_SCALAR(data, '$.id') AS STRING) AS id,                                
    CAST(JSON_EXTRACT_SCALAR(data, '$.isCategoryDefined') AS BOOL) AS is_category_defined,  
    CAST(JSON_EXTRACT_SCALAR(data, '$.isDeleted') AS BOOL) AS is_deleted,                   
    CAST(JSON_EXTRACT_SCALAR(data, '$.isGeneric') AS BOOL) AS is_generic,                   
    CAST(JSON_EXTRACT_SCALAR(data, '$.name') AS STRING) AS name,                            
    CAST(JSON_EXTRACT_ARRAY(data, '$.rules') AS ARRAY<STRING>) AS rules                     
  FROM `casla-fanhub-admin-prod.firestore_export.stream_raw_latest`                         
  WHERE document_name LIKE '%Badges%'
