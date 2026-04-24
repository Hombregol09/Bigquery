-- ============================================================================
-- Vista: Tickets
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Extrae y transforma datos de tickets desde Firestore
-- ============================================================================

WITH BaseTickets AS (                                                                              
    SELECT                                                                                           
      document_id,                                                                                   
      data                                                                                           
    FROM                                                                                             
      `casla-fanhub-admin-prod.firestore_export.stream_raw_latest`                                   
    WHERE                                                                                            
      document_name LIKE 'projects/casla-fanhub-admin-prod/databases/(default)/documents/Tickets/%'  
  )                                                                                                  
  SELECT                                                                                             
    t.document_id,                                                                                   
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.id') AS STRING                                                  
    ) AS ticket_id,                                                                                  
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.qrValue') AS STRING                                             
    ) AS qrValue,
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.eventId') AS STRING                                             
    ) AS event_id,                                                                                   
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.activityId') AS STRING                                          
    ) AS activity_id,                                                                                
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.badgeId') AS STRING                                             
    ) AS badge_id,                                                                                   
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.memberId') AS STRING                                            
    ) AS member_id,                                                                                  
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.placeId') AS STRING                                             
    ) AS place_id,                                                                                   
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.sectorId') AS STRING                                            
    ) AS sector_id,                                                                                  
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.eventPlaceSectorId') AS STRING                                  
    ) AS event_place_sector_id,                                                                      
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.category') AS STRING) AS category,
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.status') AS STRING                                              
    ) AS status,                                                                                     
    SAFE_CAST(                                                                                       
      JSON_EXTRACT_SCALAR(t.data, '$.price') AS NUMERIC                                              
    ) AS price,                                                                                      
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.isDeleted') AS BOOL                                             
    ) AS is_deleted,                                                                                 
    IFNULL(                                                                                          
      SAFE_CAST(                                                                                     
        JSON_EXTRACT_SCALAR(t.data, '$.isPass') AS BOOL                                              
      ),                                                                                             
      FALSE                                                                                          
    ) AS is_pass,                                                                                    
    IFNULL(                                                                                          
      SAFE_CAST(                                                                                     
        JSON_EXTRACT_SCALAR(t.data, '$.isTicketFreed') AS BOOL                                       
      ),                                                                                             
      FALSE                                                                                          
    ) AS is_ticket_freed,                                                                            
    IFNULL(                                                                                          
      SAFE_CAST(                                                                                     
        JSON_EXTRACT_SCALAR(                                                                         
          t.data, '$.isProvisionalOriginTicket'                                                      
        ) AS BOOL                                                                                    
      ),                                                                                             
      FALSE                                                                                          
    ) AS isProvisionalOriginTicket,                                                                  
    IFNULL(                                                                                          
      CAST(                                                                                          
        JSON_EXTRACT_SCALAR(t.data, '$.paymentId') AS STRING                                         
      ),                                                                                             
      'Sin ID de Pago'                                                                               
    ) AS payment_id,                                                                                 
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.paymentPlanId') AS STRING                                       
    ) AS paymentPlanId,                                                                              
    IFNULL(                                                                                          
      COALESCE(                                                                                      
        TIMESTAMP_SECONDS(                                                                           
          SAFE_CAST(                                                                                 
            JSON_EXTRACT_SCALAR(                                                                     
              t.data, '$.purchaseDate._seconds'                                                      
            ) AS INT64                                                                               
          )                                                                                          
        ),                                                                                           
        SAFE.PARSE_TIMESTAMP(                                                                        
          '%Y-%m-%dT%H:%M:%S%Ez',                                                                    
          JSON_EXTRACT_SCALAR(t.data, '$.purchaseDate')                                              
        ),                                                                                           
        SAFE.PARSE_TIMESTAMP(                                                                        
          '%d de %B de %Y, %I:%M:%S %p',                                                             
          REPLACE(                                                                                   
            REPLACE(                                                                                 
              REPLACE(                                                                               
                REPLACE(                                                                             
                  REPLACE(                                                                           
                    REPLACE(                                                                         
                      REPLACE(                                                                       
                        REPLACE(                                                                     
                          REPLACE(                                                                   
                            REPLACE(                                                                 
                              REPLACE(                                                               
                                REPLACE(                                                             
                                  REPLACE(                                                           
                                    REPLACE(                                                         
                                      JSON_EXTRACT_SCALAR(t.data, '$.purchaseDate'),                 
                                      ' UTC-3',                                                      
                                      ''                                                             
                                    ),                                                               
                                    'p.m.',                                                          
                                    'PM'                                                             
                                  ),                                                                 
                                  'Enero',                                                           
                                  'January'                                                          
                                ),                                                                   
                                'Febrero',                                                           
                                'February'                                                           
                              ),                                                                     
                              'Marzo',                                                               
                              'March'                                                                
                            ),                                                                       
                            'Abril',                                                                 
                            'April'                                                                  
                          ),                                                                         
                          'Mayo',                                                                    
                          'May'                                                                      
                        ),                                                                           
                        'Junio',                                                                     
                        'June'                                                                       
                      ),                                                                             
                      'Julio',                                                                       
                      'July'                                                                         
                    ),                                                                               
                    'Agosto',                                                                        
                    'August'                                                                         
                  ),                                                                                 
                  'Septiembre',                                                                      
                  'September'                                                                        
                ),                                                                                   
                'Octubre',                                                                           
                'October'                                                                            
              ),                                                                                     
              'Noviembre',                                                                           
              'November'                                                                             
            ),                                                                                       
            'Diciembre',                                                                             
            'December'                                                                               
          ),                                                                                         
          'America/Argentina/Buenos_Aires'                                                           
        )                                                                                            
      ),                                                                                             
      TIMESTAMP('1970-01-01 00:00:00 UTC')                                                           
    ) AS purchase_date_timestamp,                                                                    
    FORMAT_DATE(                                                                                     
      '%d/%m/%Y',                                                                                    
      DATE(                                                                                          
        IFNULL(                                                                                      
          COALESCE(                                                                                  
            TIMESTAMP_SECONDS(                                                                       
              SAFE_CAST(                                                                             
                JSON_EXTRACT_SCALAR(                                                                 
                  t.data, '$.purchaseDate._seconds'                                                  
                ) AS INT64                                                                           
              )                                                                                      
            ),                                                                                       
            SAFE.PARSE_TIMESTAMP(                                                                    
              '%Y-%m-%dT%H:%M:%S%Ez',                                                                
              JSON_EXTRACT_SCALAR(t.data, '$.purchaseDate')                                          
            ),                                                                                       
            SAFE.PARSE_TIMESTAMP(                                                                    
              '%d de %B de %Y, %I:%M:%S %p',                                                         
              REPLACE(                                                                               
                REPLACE(                                                                             
                  REPLACE(                                                                               
                    REPLACE(                                                                         
                      REPLACE(                                                                       
                        REPLACE(                                                                     
                          REPLACE(                                                                   
                            REPLACE(                                                                 
                              REPLACE(                                                               
                                REPLACE(                                                             
                                  REPLACE(                                                           
                                    REPLACE(                                                         
                                      REPLACE(                                                       
                                        REPLACE(                                                     
                                          JSON_EXTRACT_SCALAR(t.data, '$.purchaseDate'),             
                                          ' UTC-3',                                                  
                                          ''                                                         
                                        ),                                                           
                                        'p.m.',                                                      
                                        'PM'                                                         
                                      ),                                                             
                                      'Enero',                                                       
                                      'January'                                                      
                                    ),                                                               
                                    'Febrero',                                                       
                                    'February'                                                       
                                  ),                                                                 
                                  'Marzo',                                                           
                                  'March'                                                            
                                ),                                                                   
                                'Abril',                                                             
                                'April'                                                              
                              ),                                                                     
                              'Mayo',                                                                
                              'May'                                                                  
                            ),                                                                       
                            'Junio',                                                                 
                            'June'                                                                   
                          ),                                                                         
                          'Julio',                                                                   
                          'July'                                                                     
                        ),                                                                           
                        'Agosto',                                                                    
                        'August'                                                                     
                      ),                                                                             
                      'Septiembre',                                                                  
                      'September'                                                                    
                    ),                                                                               
                    'Octubre',                                                                       
                    'October'                                                                        
                  ),                                                                                 
                  'Noviembre',                                                                       
                  'November'                                                                         
                ),                                                                                   
                'Diciembre',                                                                         
                'December'                                                                           
              ),                                                                                     
              'America/Argentina/Buenos_Aires'                                                       
            )                                                                                        
          ),                                                                                         
          TIMESTAMP('1970-01-01 00:00:00 UTC')                                                       
        )                                                                                            
      )                                                                                              
    ) AS purchase_date_ddmmyyyy,                                                                     
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.buyerData.dni') AS STRING                                       
    ) AS buyer_dni,                                                                                  
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.buyerData.email') AS STRING                                     
    ) AS buyer_email,                                                                                
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.buyerData.memberId') AS STRING                                  
    ) AS buyer_member_id,                                                                            
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.buyerData.userType') AS STRING                                  
    ) AS buyer_user_type,                                                                            
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.seat.enabled') AS BOOL                                          
    ) AS seat_enabled,                                                                               
    CAST(                                                                                            
      JSON_EXTRACT_SCALAR(t.data, '$.seat.id') AS STRING                                             
    ) AS seat_id,                                                                                    
    SAFE_CAST(                                                                                       
      SPLIT(                                                                                         
        CAST(                                                                                        
          JSON_EXTRACT_SCALAR(t.data, '$.seat.id') AS STRING                                         
        ),                                                                                           
        '-'                                                                                          
      ) [OFFSET(0) ] AS STRING                                                                       
    ) AS seat_fila,                                                                                  
    SAFE_CAST(                                                                                       
      SPLIT(                                                                                         
        CAST(                                                                                        
          JSON_EXTRACT_SCALAR(t.data, '$.seat.id') AS STRING                                         
        ),                                                                                           
        '-'                                                                                          
      ) [OFFSET(1) ] AS STRING                                                                       
    ) AS seat_asiento                                                                                
  FROM                                                                                               
    BaseTickets AS t                                                                                 
  WHERE                                                                                              
    IFNULL(                                                                                          
      SAFE_CAST(                                                                                     
        JSON_EXTRACT_SCALAR(                                                                         
          t.data, '$.isProvisionalOriginTicket'                                                      
        ) AS BOOL                                                                                    
      ),                                                                                             
      FALSE                                                                                          
    ) = FALSE;
