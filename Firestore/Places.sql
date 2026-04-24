-- ============================================================================
-- Vista: Places
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Extrae y transforma datos de lugares desde Firestore
-- ============================================================================

SELECT                                                                                                   
    p.document_id, -- The Firestore document ID for the place                                              
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.id') AS STRING) AS place_id,                                       
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.name') AS STRING) AS place_name,                                   
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.isDeleted') AS BOOL) AS place_is_deleted,                          
    CAST(JSON_EXTRACT_SCALAR(p.data, '$.location') AS STRING) AS place_location,                           
                                                                                                           
    -- Fields from the 'sectors' array                                                                     
    CAST(JSON_EXTRACT_SCALAR(sector_data_item, '$.id') AS STRING) AS sector_id,                            
    CAST(JSON_EXTRACT_SCALAR(sector_data_item, '$.name') AS STRING) AS sector_name,                        
    CAST(JSON_EXTRACT_SCALAR(sector_data_item, '$.code') AS STRING) AS sector_code,                        
    CAST(JSON_EXTRACT_SCALAR(sector_data_item, '$.assignable') AS BOOL) AS sector_assignable,              
    CAST(JSON_EXTRACT_SCALAR(sector_data_item, '$.groupSale') AS BOOL) AS sector_group_sale,               
    CAST(JSON_EXTRACT_SCALAR(sector_data_item, '$.hasSeats') AS BOOL) AS sector_has_seats,                 
    CAST(JSON_EXTRACT_SCALAR(sector_data_item, '$.isPromotion') AS BOOL) AS sector_is_promotion,           
    SAFE_CAST(JSON_EXTRACT_SCALAR(sector_data_item, '$.realCapacity') AS NUMERIC) AS sector_real_capacity  
  FROM                                                                                                     
    `casla-fanhub-admin-prod.firestore_export.stream_raw_latest` AS p,                                     
    UNNEST(JSON_EXTRACT_ARRAY(p.data, '$.sectors')) AS sector_data_item                                    
  WHERE                                                                                                    
    p.document_name LIKE '%Places%'
