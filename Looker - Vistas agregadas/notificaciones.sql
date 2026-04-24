-- ============================================================================
-- Vista: notificaciones
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Vista agregada para notificaciones, uniendo el stream con miembros y usuarios administradores.
-- ============================================================================

WITH notifications AS (                                                                                                           
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
  )                                                                                                                                 
                                                                                                                                    
  SELECT                                                                                                                            
    n.id,                                                                                                                           
    n.creationDate_timestamp,                                                                                                       
    n.creationDate,                                                                                                                 
    n.publicationDate_timestamp,                                                                                                    
    n.publicationDate,                                                                                                              
    n.message,                                                                                                                      
    n.title,                                                                                                                        
    n.isDeleted,                                                                                                                    
    n.isRead,                                                                                                                       
                                                                                                                                    
    -- Datos del socio (members)                                                                                                    
    m.id AS member_id,                                                                                                              
    m.name AS member_nombre,                                                                                                        
    m.last_name AS member_apellido,                                                                                                 
    m.member_number AS member_numero_socio,                                                                                         
    m.email AS member_email,                                                                                                        
    m.dni,                                                                                                                          
                                                                                                                                    
    -- Datos del usuario que creó la notificación (users)                                                                           
    u.id AS user_id,                                                                                                                
    u.name AS user_nombre                                                                                                           
                                                                                                                                    
  FROM notifications n                                                                                                              
  LEFT JOIN `casla-fanhub-admin-prod.firestore_export.Members` m                                                                    
    ON n.memberId = CAST(m.id AS STRING)                                                                                            
  LEFT JOIN `casla-fanhub-admin-prod.firestore_export.Users` u                                                                      
    ON n.adminUserId = CAST(u.id AS STRING);
