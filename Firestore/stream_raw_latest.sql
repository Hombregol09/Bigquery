-- ============================================================================
-- Vista: stream_raw_latest
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Vista optimizada para obtener el último estado de cada documento en el stream de Firestore.
-- ============================================================================

SELECT                                                                        
    -- 5. Extraemos los campos del struct 'latest'                              
    latest.document_name,                                                       
    latest.document_id,                                                         
    latest.timestamp,                                                           
    latest.event_id,                                                            
    latest.operation,                                                           
    latest.data,                                                                
    latest.old_data,                                                            
    latest.path_params                                                          
  FROM (                                                                        
    SELECT                                                                      
      -- 1. Agrupamos por el nombre del documento                               
      t.document_name,                                                          
                                                                                
      -- 2. Creamos un array con *todos* los cambios de ese documento           
      ARRAY_AGG(                                                                
        -- Guardamos todas las columnas que nos interesan en un STRUCT          
        STRUCT(                                                                 
          t.document_id,                                                        
          t.timestamp,                                                          
          t.event_id,                                                           
          t.operation,                                                          
          t.data,                                                               t.old_data,                                                           
          t.path_params,                                                        
          t.document_name -- Incluimos document_name también dentro del struct  
        )                                                                       
        -- 3. Ordenamos el array por fecha, MÁS NUEVO PRIMERO                   
        ORDER BY t.timestamp DESC                                               
        -- 4. Nos quedamos solo con el primer elemento (el más nuevo)           
        LIMIT 1                                                                 
      ) [OFFSET(0)] AS latest -- Extraemos ese elemento del array               
    FROM                                                                        
      `casla-fanhub-admin-prod.firestore_export.stream_raw_changelog` AS t      
    GROUP BY                                                                    
      t.document_name                                                           
  )                                                                             
  -- 6. Finalmente, filtramos los que fueron eliminados                         
  WHERE                                                                         
    latest.operation != 'DELETE';
