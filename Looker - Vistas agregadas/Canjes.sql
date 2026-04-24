-- ============================================================================
-- Vista: Canjes
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Vista agregada para canjes, uniendo tickets con lugares, miembros y eventos.
-- ============================================================================

WITH
  UniqueTickets AS (
    SELECT * FROM (
      SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY document_id ORDER BY purchase_date_timestamp DESC) as rn
      FROM `casla-fanhub-admin-prod.firestore_export.Tickets`
    )
    WHERE rn = 1
  ),
  UniquePlaces AS (
    SELECT 
      sector_id, 
      MAX(sector_name) AS sector_name 
    FROM `casla-fanhub-admin-prod.firestore_export.Places`
    GROUP BY sector_id
  ),
  UniqueMembers AS (
    SELECT 
      id, 
      MAX(name) AS name, 
      MAX(last_name) AS last_name, 
      MAX(member_number) AS member_number,
      MAX(email) AS email, 
      MAX(phone) AS phone, 
      MAX(dni) AS dni
    FROM `casla-fanhub-admin-prod.firestore_export.Members`
    GROUP BY id
  ),
  UniqueEvents AS (
    SELECT 
      event_id, 
      MAX(description) AS description
    FROM `casla-fanhub-admin-prod.firestore_export.Events`
    GROUP BY event_id
  )

SELECT                                                                               
    t.document_id,                                                                     
    t.activity_id,                                                                     
    t.badge_id,                                                                        
    t.event_id,                                                                        
    e.description,                                                                     
    t.event_place_sector_id,                                                           
    t.ticket_id,                                                                       
    t.is_deleted,                                                                      
    t.is_pass,                                                                         
    t.is_ticket_freed,                                                                 
    t.payment_id,                                                                      
    t.place_id,                                                                        
    t.member_id,                                                                       
    t.paymentPlanId,                                                                   
    t.price,                                                                           
    t.purchase_date_timestamp,                                                         
    PARSE_DATE('%d/%m/%Y', t.purchase_date_ddmmyyyy) AS purchase_date_correct_format,  
    t.sector_id,                                                                       
    t.buyer_dni,                                                                       
    t.buyer_email,                                                                     
    t.buyer_member_id,                                                                 
    t.buyer_user_type,                                                                 
    t.seat_enabled,                                                                    
    t.seat_id,                                                                         
    t.seat_fila,                                                                       
    t.seat_asiento,                                                                    
    t.category,                                                                        
    t.status,                                                                          
    p.sector_name AS place_sector_name,                                                
    m.name AS member_name,                                                             
    m.last_name AS member_lastname,                                                    
    m.member_number AS member_number,                                                  
    m.email AS member_email,                                                           
    m.phone AS member_phone,                                                           
    m.dni AS member_dni                                                                
  FROM                                                                                 
    UniqueTickets AS t                            
  LEFT JOIN                                                                            
    UniquePlaces AS p                             
    ON t.sector_id = p.sector_id                                                       
  LEFT JOIN                                                                            
    UniqueMembers AS m                            
    ON t.member_id = m.id                                                              
  LEFT JOIN                                                                            
    UniqueEvents AS e                             
   ON t.event_id = e.event_id;
