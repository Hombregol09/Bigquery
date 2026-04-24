-- ============================================================================
-- Vista: cierre_de_caja
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Vista agregada para cierre de caja, uniendo tickets con usuarios, lugares, pagos y miembros.
-- ============================================================================

WITH                                                                                                                                                                                     
    TicketsPackage_Cleaned AS (                                                                                                                                                            
      SELECT                                                                                                                                                                               
        document_id AS tickets_package_document_id,                                                                                                                                        
        final_total AS importe_final_descuentos,                                                                                                                                           
        promo_discount_by_payment_method AS promocion_aplicada,                                                                                                                            
        payment_method AS metodos_de_pago,                                                                                                                                                 
        total_tickets_in_package AS cantidad_de_tickets,                                                                                                                                   
        created_date,                                                                                                                                                                      
        admin_user_id,                                                                                                                                                                     
        sector_id AS tickets_package_sector_id,                                                                                                                                            
        paymentPlanId AS Payments_Relation_ID,                                                                                                                                             
        memberId AS Members_Relation_ID,                                                                                                                                                   
        is_deleted AS tickets_package_is_deleted,                                                                                                                                          
        status,                                                                                                                                                                            
        event_id AS event_id                                                                                                                                                               
      FROM                                                                                                                                                                                 
        `casla-fanhub-admin-prod.firestore_export.TicketsPackage`                                                                                                                          
    ),                                                                                                                                                                                     
    UniqueTicketsPackage AS (                                                                                                                                                              
      SELECT                                                                                                                                                                               
        tickets_package_document_id,                                                                                                                                                       
        importe_final_descuentos,                                                                                                                                                          
        promocion_aplicada,                                                                                                                                                                
        metodos_de_pago,                                                                                                                                                                   
        cantidad_de_tickets,                                                                                                                                                               
        created_date,                                                                                                                                                                      
        admin_user_id,                                                                                                                                                                     
        tickets_package_sector_id,                                                                                                                                                         
        Payments_Relation_ID,                                                                                                                                                              
        Members_Relation_ID,                                                                                                                                                               
        tickets_package_is_deleted,                                                                                                                                                        
        status,                                                                                                                                                                            
        event_id                                                                                                                                                                           
      FROM (                                                                                                                                                                               
        SELECT                                                                                                                                                                             
          *,                                                                                                                                                                               
          ROW_NUMBER() OVER (PARTITION BY tickets_package_document_id ORDER BY created_date) as rn                                                                                         
        FROM                                                                                                                                                                               
          TicketsPackage_Cleaned                                                                                                                                                           
      )                                                                                                                                                                                    
      WHERE rn = 1                                                                                                                                                                         
    ),                                                                                                                                                                                     
    -- CTEs para eliminar duplicados de las tablas unidas, manteniendo los campos de la query original
    UniqueUsers AS (
      SELECT id, MAX(name) AS name FROM `casla-fanhub-admin-prod.firestore_export.Users` GROUP BY id
    ),
    UniquePlaces AS (
      SELECT sector_id, MAX(sector_name) AS sector_name, MAX(sector_code) AS sector_code FROM `casla-fanhub-admin-prod.firestore_export.Places` GROUP BY sector_id
    ),
    UniquePayments AS (
      SELECT id, MAX(name) AS name FROM `casla-fanhub-admin-prod.firestore_export.Payments` GROUP BY id
    ),
    UniqueMembers AS (
      SELECT id, MAX(name) AS name, MAX(last_name) AS last_name, MAX(phone) AS phone, MAX(email) AS email, MAX(dni) AS dni, MAX(member_number) AS member_number FROM `casla-fanhub-admin-prod.firestore_export.Members` GROUP BY id
    )                                                                                                                                                                                      
  SELECT                                                                                                                                                                                   
    tp.tickets_package_document_id,                                                                                                                                                        
    tp.event_id,                                                                                                                                                                           
    tp.importe_final_descuentos,                                                                                                                                                           
    tp.cantidad_de_tickets,                                                                                                                                                                
    tp.promocion_aplicada,                                                                                                                                                                 
    tp.metodos_de_pago,                                                                                                                                                                    
    p.name AS `Plan_de_Pagos`,                                                                                                                                                             
    tp.created_date AS fecha_de_creacion,                                                                                                                                                  
    u.name AS usuario_que_hizo_la_venta,                                                                                                                                                   
    pl.sector_name AS sector_del_estadio,                                                                                                                                                  
    pl.sector_code AS codigo_sector_del_estadio,                                                                                                                                           
    m.name AS member_name,                                                                                                                                                                 
    m.last_name AS member_lastname,                                                                                                                                                        
    m.phone AS member_phone,                                                                                                                                                               
    m.email AS member_email,                                                                                                                                                               
    m.dni AS member_dni,                                                                                                                                                                   
    m.member_number AS member_number,                                                                                                                                                      
    tp.tickets_package_is_deleted,                                                                                                                                                         
    tp.status AS tickets_package_status                                                                                                                                                    
  FROM                                                                                                                                                                                     
    UniqueTicketsPackage AS tp                                                                                                                                                             
  LEFT JOIN                                                                                                                                                                                
    UniqueUsers AS u                                                                                                                                                                       
    ON tp.admin_user_id = u.id                                                                                                                                                             
  LEFT JOIN                                                                                                                                                                                
    UniquePlaces AS pl                                                                                                                                                                     
    ON tp.tickets_package_sector_id = pl.sector_id                                                                                                                                         
  LEFT JOIN                                                                                                                                                                                
    UniquePayments AS p                                                                                                                                                                    
    ON tp.Payments_Relation_ID = p.id                                                                                                                                                      
  LEFT JOIN                                                                                                                                                                                
    UniqueMembers AS m                                                                                                                                                                     
    ON tp.Members_Relation_ID = m.id;
