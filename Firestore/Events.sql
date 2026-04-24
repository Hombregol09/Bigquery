-- ============================================================================
-- Vista: Events
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Extrae y transforma datos de eventos desde Firestore
-- ============================================================================

SELECT                                                                                                   
    document_id AS event_id,                                                                               
    CAST(JSON_EXTRACT_SCALAR(data, '$.activityId') AS STRING) AS activity_id,                              
    CAST(JSON_EXTRACT_SCALAR(data, '$.adminUserId') AS STRING) AS admin_user_id,                           
    CAST(JSON_EXTRACT_SCALAR(data, '$.description') AS STRING) AS description,                             
    TIMESTAMP_SECONDS(CAST(JSON_EXTRACT_SCALAR(data, '$.eventDate._seconds') AS INT64)) AS event_date,      
    TIMESTAMP_SECONDS(CAST(JSON_EXTRACT_SCALAR(data, '$.creationDate._seconds') AS INT64)) AS creation_date,  
    TIMESTAMP_SECONDS(CAST(JSON_EXTRACT_SCALAR(data, '$.eventStartReleaseDate._seconds') AS INT64)) AS event_start_release_date,  
    TIMESTAMP_SECONDS(CAST(JSON_EXTRACT_SCALAR(data, '$.eventEndReleaseDate._seconds') AS INT64)) AS event_end_release_date,    
                                                                                                           
    -- eventItems como ARRAY de STRUCT                                                                     
    (                                                                                                      
      SELECT ARRAY_AGG(STRUCT(                                                                             
        CAST(JSON_EXTRACT_SCALAR(item, '$.badgeType') AS STRING) AS badge_type,                            
        CAST(JSON_EXTRACT_SCALAR(item, '$.eventSector') AS STRING) AS event_sector,                        
        CAST(JSON_EXTRACT_SCALAR(item, '$.id') AS STRING) AS event_item_id,                                
        CAST(JSON_EXTRACT_SCALAR(item, '$.isSelfManaged') AS BOOL) AS is_self_managed,                     
        CAST(JSON_EXTRACT_SCALAR(item, '$.maxOrdersPerUser') AS INT64) AS max_orders_per_user,             
        CAST(JSON_EXTRACT_SCALAR(item, '$.paymentPlans') AS STRING) AS payment_plans,                      
        CAST(JSON_EXTRACT_SCALAR(item, '$.realCapacity') AS INT64) AS real_capacity,                       
        CAST(JSON_EXTRACT_SCALAR(item, '$.realSalesCapacity') AS INT64) AS real_sales_capacity             
      ))                                                                                                   
      FROM UNNEST(JSON_EXTRACT_ARRAY(data, '$.eventItems')) AS item                                        
    ) AS eventItems                                                                                        
                                                                                                           
  FROM `casla-fanhub-admin-prod.firestore_export.stream_raw_latest`                                        
  WHERE document_name LIKE '%Events%';
