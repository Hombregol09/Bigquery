-- ============================================================================
-- Vista: Reportes
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Vista agregada para reportes de ventas de paquetes de tickets.
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
        payment_id,                                                                   
        admin_user_id,                                                                
        sector_id AS tickets_package_sector_id,                                       
        LOWER(TRIM(package_ticket_item_id)) AS package_ticket_item_id_cleaned,        
        ticket_package_id                                                             
      FROM                                                                            
        `casla-fanhub-admin-prod.firestore_export.TicketsPackage`                     
    ),                                                                                
    Tickets_Cleaned AS (                                                              
      SELECT                                                                          
        document_id AS ticket_document_id,                                            
        LOWER(TRIM(ticket_id)) AS ticket_id_cleaned,                                  
        seat_fila,                                                                    
        seat_asiento                                                                  
      FROM                                                                            
        `casla-fanhub-admin-prod.firestore_export.Tickets`                            
    )                                                                                 
  SELECT                                                                              
    tp.tickets_package_document_id,                                                   
    tp.importe_final_descuentos,                                                      
    tp.promocion_aplicada,                                                            
    tp.metodos_de_pago,                                                               
    tp.cantidad_de_tickets,                                                           
    tp.created_date AS fecha_de_creacion,                                             
    u.name AS usuario_que_hizo_la_venta,                                              
    pl.sector_name AS sector_del_estadio,                                             
    pl.sector_code AS codigo_sector_del_estadio,                                      
    tc.seat_fila AS fila,                                                             
    tc.seat_asiento AS asiento                                                        
  FROM                                                                                
    TicketsPackage_Cleaned AS tp                                                      
  LEFT JOIN                                                                           
    `casla-fanhub-admin-prod.firestore_export.Users` AS u                             
    ON tp.admin_user_id = u.id                                                        
  LEFT JOIN                                                                           
    `casla-fanhub-admin-prod.firestore_export.Places` AS pl                           
    ON tp.tickets_package_sector_id = pl.sector_id                                    
  LEFT JOIN                                                                           
    Tickets_Cleaned AS tc                                                             
    ON tp.package_ticket_item_id_cleaned = tc.ticket_id_cleaned;
