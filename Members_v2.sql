-- ============================================================================
-- Vista: Members_v2
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Versión optimizada con deduplicación liviana sobre stream_raw_changelog
-- ============================================================================

WITH BaseDocumento AS (
    SELECT 
      document_id,
      data
    FROM `casla-fanhub-admin-prod.firestore_export.stream_raw_changelog`
    WHERE document_name LIKE 'projects/casla-fanhub-admin-prod/databases/(default)/documents/Members/%'
    -- 1. Descartamos los borrados que hayan ocurrido hoy
    AND operation != 'DELETE' 
    -- 2. Deduplicación liviana: nos quedamos solo con el último estado del día
    QUALIFY ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY timestamp DESC) = 1 
)
SELECT                                                                                                   
    t.document_id,                                                                                           
                                                                                                           
    -- ✅ activationAccount: formato 'YYYY-MM-DD HH:MM'                                                    
    FORMAT_TIMESTAMP(                                                                                      
      '%Y-%m-%d %H:%M',                                                                                    
      COALESCE(                                                                                            
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_EXTRACT_SCALAR(t.data, '$.activationAccount._seconds') AS INT64)),  
        SAFE_CAST(JSON_EXTRACT_SCALAR(t.data, '$.activationAccount') AS TIMESTAMP)                           
      ),                                                                                                   
      'America/Argentina/Buenos_Aires'                                                                     
    ) AS activationAccount,                                                                                
                                                                                                           
    CAST(JSON_EXTRACT_SCALAR(t.data, '$.address') AS STRING) AS address,                                     
    CAST(JSON_EXTRACT_ARRAY(t.data, '$.badges') AS ARRAY<STRING>) AS badges,                                 
                                                                                                           
    -- ✅ birthdate: formato 'YYYY-MM-DD'                                                                  
    FORMAT_DATE(                                                                                           
      '%Y-%m-%d',                                                                                          
      DATE(                                                                                                
        COALESCE(                                                                                          
          TIMESTAMP_SECONDS(SAFE_CAST(JSON_EXTRACT_SCALAR(t.data, '$.birthdate._seconds') AS INT64)),        
          SAFE_CAST(JSON_EXTRACT_SCALAR(t.data, '$.birthdate') AS TIMESTAMP)                                 
        ),                                                                                                 
        'America/Argentina/Buenos_Aires'                                                                   
      )                                                                                                    
    ) AS birthdate,                                                                                        
                                                                                                           
    CAST(JSON_EXTRACT_SCALAR(t.data, '$.city') AS STRING) AS city,                                           
    CAST(JSON_EXTRACT_SCALAR(t.data, '$.country') AS STRING) AS country,                                     
    CAST(JSON_EXTRACT_SCALAR(t.data, '$.dni') AS STRING) AS dni,                                             
    CAST(JSON_EXTRACT_SCALAR(t.data, '$.email') AS STRING) AS email,                                         
    CAST(JSON_EXTRACT_SCALAR(t.data, '$.id') AS STRING) AS id,                                               
    CAST(JSON_EXTRACT_SCALAR(t.data, '$.isDeleted') AS BOOL) AS is_deleted,                                  
    CAST(JSON_EXTRACT_SCALAR(t.data, '$.lastName') AS STRING) AS last_name,                                  
    CAST(JSON_EXTRACT_ARRAY(t.data, '$.lastNameNormalized') AS ARRAY<STRING>) AS last_name_normalized,       
    CAST(JSON_EXTRACT_SCALAR(t.data, '$.memberNumber') AS STRING) AS member_number,                          
    CAST(JSON_EXTRACT_ARRAY(t.data, '$.memberships') AS ARRAY<STRING>) AS memberships,                       
    CAST(JSON_EXTRACT_SCALAR(t.data, '$.name') AS STRING) AS name,                                           
    CAST(JSON_EXTRACT_SCALAR(t.data, '$.phone') AS STRING) AS phone,                                         
    CAST(JSON_EXTRACT_SCALAR(t.data, '$.profile') AS STRING) AS profile,                                     
    CAST(JSON_EXTRACT_SCALAR(t.data, '$.province') AS STRING) AS province,                                   
    CAST(JSON_EXTRACT_SCALAR(t.data, '$.uid') AS STRING) AS uid,                                             
    CAST(JSON_EXTRACT_SCALAR(t.data, '$.familyGroupId') AS STRING) AS familyGroupId                          
                                                                                                           
  FROM                                                                                                     
    BaseDocumento AS t;
