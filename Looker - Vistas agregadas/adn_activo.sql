-- ============================================================================
-- Vista: adn_activo
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Vista de miembros con estado de activación de cuenta y pertenencia a ADN.
-- ============================================================================

WITH users_emails AS (                                                                                                          
    SELECT DISTINCT                                                                                                               
      email                                                                                                                       
    FROM `casla-fanhub-admin-prod.firestore_export.Users`                                                                         
    WHERE email IS NOT NULL                                                                                                       
      AND is_deleted IS NOT TRUE                                                                                                  
  ),                                                                                                                              
                                                                                                                                  
  all_members_with_app_status AS (                                                                                                
    SELECT                                                                                                                        
      -- Email para join y salida                                                                                                 
      CAST(JSON_EXTRACT_SCALAR(m.data, '$.email') AS STRING) AS email,                                                            
                                                                                                                                  
      -- TIMESTAMP de activación                                                                                               
      COALESCE(                                                                                                                   
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_EXTRACT_SCALAR(m.data, '$.activationAccount._seconds') AS INT64)),                       
        SAFE_CAST(JSON_EXTRACT_SCALAR(m.data, '$.activationAccount') AS TIMESTAMP)                                                
      ) AS activationAccount_ts,                                                                                                  
                                                                                                                                  
      -- Limpieza de nombre y apellido                                                                                            
      LOWER(TRIM(REGEXP_REPLACE(CAST(JSON_EXTRACT_SCALAR(m.data, '$.name') AS STRING), r'["\']', ''))) AS name_cleaned,           
      LOWER(TRIM(REGEXP_REPLACE(CAST(JSON_EXTRACT_SCALAR(m.data, '$.lastName') AS STRING), r'["\']', ''))) AS last_name_cleaned,  
                                                                                                                                  
      -- DNI limpio                                                                                                               
      REGEXP_REPLACE(CAST(JSON_EXTRACT_SCALAR(m.data, '$.dni') AS STRING), r'[^0-9]', '') AS dni_cleaned,                         
                                                                                                                                  
      -- Otros campos                                                                                                             
      CAST(JSON_EXTRACT_SCALAR(m.data, '$.memberNumber') AS STRING) AS member_number,                                             
      CAST(JSON_EXTRACT_SCALAR(m.data, '$.id') AS STRING) AS document_id,                                                         
      CAST(JSON_EXTRACT_SCALAR(m.data, '$.phone') AS STRING) AS phone,                                                            
      CAST(JSON_EXTRACT_SCALAR(m.data, '$.city') AS STRING) AS city,                                                              
      CAST(JSON_EXTRACT_SCALAR(m.data, '$.province') AS STRING) AS province,                                                      
      CAST(JSON_EXTRACT_SCALAR(m.data, '$.country') AS STRING) AS country,                                                        
      CAST(JSON_EXTRACT_SCALAR(m.data, '$.uid') AS STRING) AS uid,                                                                
                                                                                                                                  
      -- SOCIO_ADN                                                                                                                
      CASE                                                                                                                        
        WHEN CAST(JSON_EXTRACT_SCALAR(m.data, '$.uid') AS STRING) IS NOT NULL                                                     
             AND u.email IS NULL THEN 'SI'                                                                                        
        ELSE 'NO'                                                                                                                 
      END AS SOCIO_ADN                                                                                                            
                                                                                                                                  
    FROM `casla-fanhub-admin-prod.firestore_export.stream_raw_latest` AS m                                                        
    LEFT JOIN users_emails AS u                                                                                                   
      ON CAST(JSON_EXTRACT_SCALAR(m.data, '$.email') AS STRING) = u.email                                                         
    WHERE                                                                                                                         
      CAST(JSON_EXTRACT_SCALAR(m.data, '$.isDeleted') AS BOOL) IS NOT TRUE                                                        
  )                                                                                                                               
                                                                                                                                  
  SELECT                                                                                                                          
    email,                                                                                                                        
                                                                                                                                  
    -- Fecha + hora legible                                                                                                    
    FORMAT_TIMESTAMP(                                                                                                             
      '%Y-%m-%d %H:%M',                                                                                                           
      activationAccount_ts,                                                                                                       
      'America/Argentina/Buenos_Aires'                                                                                            
    ) AS fecha_activacion,                                                                                                        
                                                                                                                                  
    -- Solo fecha                                                                                                              
    DATE(activationAccount_ts, 'America/Argentina/Buenos_Aires') AS fecha_activacion_dia,                                         
                                                                                                                                  
    name_cleaned AS nombre,                                                                                                       
    last_name_cleaned AS apellido,                                                                                                
    dni_cleaned AS dni,                                                                                                           
    member_number AS numero_socio,                                                                                                
                                                                                                                                  
    CASE                                                                                                                          
      WHEN UPPER(city) = 'BUENOS AIRES' OR UPPER(province) = 'CAPITAL FEDERAL' THEN province                                      
      ELSE city                                                                                                                   
    END AS ciudad,                                                                                                                
                                                                                                                                  
    CASE                                                                                                                          
      WHEN UPPER(city) = 'BUENOS AIRES' OR UPPER(province) = 'CAPITAL FEDERAL' THEN 'Buenos Aires'                                
      ELSE province                                                                                                               
    END AS provincia,                                                                                                             
                                                                                                                                  
    country AS pais,                                                                                                              
    SOCIO_ADN,                                                                                                                    
    document_id AS member_id,                                                                                                     
    phone AS telefono                                                                                                             
                                                                                                                                  
  FROM all_members_with_app_status                                                                                                
  WHERE                                                                                                                           
    member_number IS NOT NULL                                                                                                     
  ORDER BY                                                                                                                        
    numero_socio, nombre, apellido;
