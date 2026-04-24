-- ============================================================================
-- Vista: insignias
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Vista de miembros con sus insignias (badges) y metadatos asociados.
-- ============================================================================

-- Stream (latest) + Members (solo badges filtra por ids del stream) + Badges (meta)                                                       
WITH users_emails AS (                                                                                                                     
    SELECT DISTINCT email                                                                                                                    
    FROM `casla-fanhub-admin-prod.firestore_export.Users`                                                                                    
    WHERE email IS NOT NULL                                                                                                                  
      AND is_deleted IS NOT TRUE                                                                                                             
  ),                                                                                                                                         
                                                                                                                                              
  -- 1) Extraigo desde stream_raw_latest TODO lo que necesito (activationAccount correcto)                                                   
  clean_stream AS (                                                                                                                          
    SELECT                                                                                                                                   
      CAST(JSON_EXTRACT_SCALAR(s.data, '$.email') AS STRING)                    AS email,                                                    
      CAST(JSON_EXTRACT_SCALAR(s.data, '$.id') AS STRING)                       AS member_id, -- id en el JSON del stream                    
      -- Activation: mismo método que decías que funciona                                                                                    
      COALESCE(                                                                                                                              
        TIMESTAMP_SECONDS(SAFE_CAST(JSON_EXTRACT_SCALAR(s.data, '$.activationAccount._seconds') AS INT64)),                                  
        SAFE_CAST(JSON_EXTRACT_SCALAR(s.data, '$.activationAccount') AS TIMESTAMP)                                                           
      )                                                                         AS activationAccount_ts,                                     
      LOWER(TRIM(REGEXP_REPLACE(CAST(JSON_EXTRACT_SCALAR(s.data, '$.name') AS STRING), r'["\']', ''))) AS name_cleaned,                      
      LOWER(TRIM(REGEXP_REPLACE(CAST(JSON_EXTRACT_SCALAR(s.data, '$.lastName') AS STRING), r'["\']', ''))) AS last_name_cleaned,             
      REGEXP_REPLACE(CAST(JSON_EXTRACT_SCALAR(s.data, '$.dni') AS STRING), r'[^0-9]', '')                 AS dni_cleaned,                    
      CAST(JSON_EXTRACT_SCALAR(s.data, '$.memberNumber') AS STRING)                              AS member_number,                           
      CAST(JSON_EXTRACT_SCALAR(s.data, '$.id') AS STRING)                                         AS document_id, -- coincide con member_id  
      CAST(JSON_EXTRACT_SCALAR(s.data, '$.phone') AS STRING)                                      AS phone,                                  
      CAST(JSON_EXTRACT_SCALAR(s.data, '$.city') AS STRING)                                       AS city,                                   
      CAST(JSON_EXTRACT_SCALAR(s.data, '$.province') AS STRING)                                   AS province,                               
      CAST(JSON_EXTRACT_SCALAR(s.data, '$.country') AS STRING)                                    AS country,                                
      CAST(JSON_EXTRACT_SCALAR(s.data, '$.uid') AS STRING)                                        AS uid,                                    
      CASE WHEN CAST(JSON_EXTRACT_SCALAR(s.data, '$.uid') AS STRING) IS NOT NULL                                                             
                AND ue.email IS NULL THEN 'SI' ELSE 'NO' END                                    AS SOCIO_ADN                                 
    FROM `casla-fanhub-admin-prod.firestore_export.stream_raw_latest` AS s                                                                   
    LEFT JOIN users_emails ue                                                                                                                
      ON CAST(JSON_EXTRACT_SCALAR(s.data, '$.email') AS STRING) = ue.email                                                                   
    WHERE CAST(JSON_EXTRACT_SCALAR(s.data, '$.isDeleted') AS BOOL) IS NOT TRUE                                                               
  ),                                                                                                                                         
                                                                                                                                              
  -- 2) Lista de member_ids que necesitamos (reduce el scope de members)                                                                     
  needed_member_ids AS (                                                                                                                     
    SELECT DISTINCT member_id                                                                                                                
    FROM clean_stream                                                                                                                        
    WHERE member_id IS NOT NULL                                                                                                              
  ),                                                                                                                                         
                                                                                                                                              
  -- 3) Traigo SOLO rows de Members necesarias (solo id + badges) para no escanear todo                                                      
  members_filtered AS (                                                                                                                      
    SELECT                                                                                                                                   
      id AS id_members,                                                                                                                      
      badges                                                                                                                                 
    FROM `casla-fanhub-admin-prod.firestore_export.Members`                                                                                  
    WHERE id IN (SELECT member_id FROM needed_member_ids)                                                                                    
      AND is_deleted IS NOT TRUE                                                                                                             
  ),                                                                                                                                         
                                                                                                                                              
  -- 4) Expandir badges por fila y limpiar badge id                                                                                          
  members_badges_expanded AS (                                                                                                               
    SELECT                                                                                                                                   
      mf.id_members AS member_id,                                                                                                            
      TRIM(REGEXP_REPLACE(b, r'["\']', '')) AS badge_id_cleaned                                                                              
    FROM members_filtered mf                                                                                                                 
    LEFT JOIN UNNEST(mf.badges) AS b                                                                                                         
  ),                                                                                                                                         
                                                                                                                                              
  -- 5) Traer metadata de badges (nombre, descripcion)                                                                                       
  members_badges_with_meta AS (                                                                                                              
    SELECT                                                                                                                                   
      mbe.member_id,                                                                                                                         
      mbe.badge_id_cleaned AS badge_id,                                                                                                      
      b.name AS badge_nombre,                                                                                                                
      b.description AS badge_descripcion                                                                                                     
    FROM members_badges_expanded mbe                                                                                                         
    LEFT JOIN `casla-fanhub-admin-prod.firestore_export.Badges` b                                                                            
      ON mbe.badge_id_cleaned = b.id                                                                                                         
      AND b.is_deleted IS NOT TRUE                                                                                                           
  )                                                                                                                                          
                                                                                                                                              
  -- 6) Resultado final: stream + badges (una fila por badge). Si no tiene badges, aparecen NULLs.                                           
  SELECT                                                                                                                                     
    s.email,                                                                                                                                 
                                                                                                                                             
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', s.activationAccount_ts, 'America/Argentina/Buenos_Aires') AS fecha_activacion,                        
    DATE(s.activationAccount_ts, 'America/Argentina/Buenos_Aires')                             AS fecha_activacion_dia,                      
                                                                                                                                             
    s.name_cleaned AS nombre,                                                                                                                
    s.last_name_cleaned AS apellido,                                                                                                         
    s.dni_cleaned AS dni,                                                                                                                    
    s.member_number AS numero_socio,                                                                                                         
                                                                                                                                             
    CASE WHEN UPPER(s.city) = 'BUENOS AIRES' OR UPPER(s.province) = 'CAPITAL FEDERAL' THEN s.province ELSE s.city END AS ciudad,             
    CASE WHEN UPPER(s.city) = 'BUENOS AIRES' OR UPPER(s.province) = 'CAPITAL FEDERAL' THEN 'Buenos Aires' ELSE s.province END AS provincia,  
                                                                                                                                             
    s.country AS pais,                                                                                                                       
    s.SOCIO_ADN,                                                                                                                             
    s.document_id AS member_id,                                                                                                              
    s.phone AS telefono,                                                                                                                     
                                                                                                                                             
    mbwm.badge_id AS badge_id,                                                                                                               
    mbwm.badge_nombre AS badge_nombre,                                                                                                       
    mbwm.badge_descripcion AS badge_descripcion                                                                                              
                                                                                                                                             
  FROM clean_stream s                                                                                                                        
  LEFT JOIN members_badges_with_meta mbwm                                                                                                    
    ON s.document_id = mbwm.member_id                                                                                                        
                                                                                                                                             
  WHERE s.member_number IS NOT NULL                                                                                                          
                                                                                                                                             
  ORDER BY s.member_number, s.name_cleaned, s.last_name_cleaned, mbwm.badge_id;
