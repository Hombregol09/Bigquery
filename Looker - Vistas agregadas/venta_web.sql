-- ============================================================================
-- Vista: venta_web
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Vista agregada para ventas web extraídas de PagoticPayments.
-- ============================================================================

WITH UniqueMembers AS (
    SELECT 
        id, 
        MAX(name) AS name, 
        MAX(last_name) AS last_name, 
        MAX(phone) AS phone, 
        MAX(email) AS email, 
        MAX(member_number) AS member_number,
        MAX(dni) AS dni
    FROM `casla-fanhub-admin-prod.firestore_export.Members`
    GROUP BY id
),
UniquePlaces AS (
    SELECT 
        sector_id, 
        MAX(sector_name) AS sector_name, 
        MAX(sector_code) AS sector_code, 
        MAX(place_name) AS name_location, 
        MAX(place_location) AS adress_location
    FROM `casla-fanhub-admin-prod.firestore_export.Places`
    GROUP BY sector_id
),
UniquePayments AS (
    SELECT 
        id, 
        MAX(name) AS name
    FROM `casla-fanhub-admin-prod.firestore_export.Payments`
    GROUP BY id
)

SELECT DISTINCT                                                     
    pp.updated_at_ddmmyyyy AS purchase_date,                          
    pp.pagotic_id,                                                    
    pp.description,                                                   
    pp.amount,                                                        
    COALESCE(pp.email, m.email) AS email,                             
    pp.dni,                                                           
    pp.document_id,                                                   
    pp.event,                                                         
    pp.payment_type,                                                  
    pp.brand,                                                         
    pp.installment_amount,                                            
    pp.installments,                                                  
    pp.status AS payment_status,                                      
    pp.tickets_count AS cantidad_tickets,                             
    pp.event_description,                                             
    pl.sector_name AS sector_name,                                    
    p.name AS plan_de_pagos,                                          
    m.name AS member_name,                                            
    m.last_name AS member_lastname,                                   
    m.member_number AS member_number,                                 
    m.email AS member_email,                                          
    m.phone AS member_phone                                           
                                                                      
  FROM                                                                
    `casla-fanhub-admin-prod.firestore_export.PagoticPayments` AS pp  
                                                                      
  LEFT JOIN                                                           
    UniqueMembers AS m           
    ON pp.buyerMemberId = m.id                                        
                                                                      
  LEFT JOIN                                                           
    UniquePlaces AS pl           
    ON pp.sector_id = pl.sector_id                                    
                                                                      
  LEFT JOIN                                                           
    UniquePayments AS p          
    ON pp.payment_plan_id = p.id                                      
                                                                      
  WHERE                                                               
    pp.buyerMemberId IS NOT NULL;
