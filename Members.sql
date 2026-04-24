-- ============================================================================
-- Vista: Members
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Extrae y transforma datos de miembros desde Firestore
-- ============================================================================

SELECT                                                                                                   
    document_id,                                                                                           
                                                                                                           
    -- ✅ activationAccount: formato 'YYYY-MM-DD HH:MM'                                                    
    FORMAT_TIMESTAMP(                                                                                      
      '%Y-%m-%d %H:%M',                                                                                    
      COALESCE(                                                                                            
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.activationAccount._seconds') AS INT64)),  
        SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.activationAccount') AS TIMESTAMP)                           
      ),                                                                                                   
      'America/Argentina/Buenos_Aires'                                                                     
    ) AS activationAccount,                                                                                
                                                                                                           
    CAST(JSON_EXTRACT_SCALAR(data, '$.address') AS STRING) AS address,                                     
    CAST(JSON_EXTRACT_ARRAY(data, '$.badges') AS ARRAY<STRING>) AS badges,                                 
                                                                                                           
    -- ✅ birthdate: formato 'YYYY-MM-DD'                                                                  
    FORMAT_DATE(                                                                                           
      '%Y-%m-%d',                                                                                          
      DATE(                                                                                                
        COALESCE(                                                                                          
          TIMESTAMP_SECONDS(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.birthdate._seconds') AS INT64)),        
          SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.birthdate') AS TIMESTAMP)                                 
        ),                                                                                                 
        'America/Argentina/Buenos_Aires'                                                                   
      )                                                                                                    
    ) AS birthdate,                                                                                        
                                                                                                           
    CAST(JSON_EXTRACT_SCALAR(data, '$.city') AS STRING) AS city,                                           
    CAST(JSON_EXTRACT_SCALAR(data, '$.country') AS STRING) AS country,                                     
    CAST(JSON_EXTRACT_SCALAR(data, '$.dni') AS STRING) AS dni,                                             
    CAST(JSON_EXTRACT_SCALAR(data, '$.email') AS STRING) AS email,                                         
    CAST(JSON_EXTRACT_SCALAR(data, '$.id') AS STRING) AS id,                                               
    CAST(JSON_EXTRACT_SCALAR(data, '$.isDeleted') AS BOOL) AS is_deleted,                                  
    CAST(JSON_EXTRACT_SCALAR(data, '$.lastName') AS STRING) AS last_name,                                  
    CAST(JSON_EXTRACT_ARRAY(data, '$.lastNameNormalized') AS ARRAY<STRING>) AS last_name_normalized,       
    CAST(JSON_EXTRACT_SCALAR(data, '$.memberNumber') AS STRING) AS member_number,                          
    CAST(JSON_EXTRACT_ARRAY(data, '$.memberships') AS ARRAY<STRING>) AS memberships,                       
    CAST(JSON_EXTRACT_SCALAR(data, '$.name') AS STRING) AS name,                                           
    CAST(JSON_EXTRACT_SCALAR(data, '$.phone') AS STRING) AS phone,                                         
    CAST(JSON_EXTRACT_SCALAR(data, '$.profile') AS STRING) AS profile,                                     
    CAST(JSON_EXTRACT_SCALAR(data, '$.province') AS STRING) AS province,                                   
    CAST(JSON_EXTRACT_SCALAR(data, '$.uid') AS STRING) AS uid,                                             
    CAST(JSON_EXTRACT_SCALAR(data, '$.familyGroupId') AS STRING) AS familyGroupId                          
                                                                                                           
  FROM                                                                                                     
    `casla-fanhub-admin-prod.firestore_export.stream_raw_latest`                                           
  WHERE                                                                                                    
    document_name LIKE '%Members%';
