-- ============================================================================
-- Vista: family_groups
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Extrae y transforma datos de grupos familiares desde Firestore
-- ============================================================================

SELECT                                                                                      
    document_id AS id,                                                                        
    JSON_EXTRACT_SCALAR(data, '$.fields.createdAt.timestampValue') AS createdAt,              
    CAST(JSON_EXTRACT_SCALAR(data, '$.fields.isDeleted.booleanValue') AS BOOL) AS isDeleted,  
    JSON_EXTRACT_ARRAY(data, '$.fields.members.arrayValue.values') AS members                 
  FROM                                                                                        
    `casla-fanhub-admin-prod.firestore_export.stream_raw_latest`                              
  WHERE                                                                                       
    document_name LIKE '%/FamilyGroups/%'
