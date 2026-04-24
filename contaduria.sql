-- ============================================================================
-- Vista: contaduria
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Vista agregada para contaduría, uniendo pagos con miembros y paquetes de tickets.
-- ============================================================================

SELECT DISTINCT                                                     
    pp.updated_at_ddmmyyyy AS purchase_date,                          
    pp.pagotic_id,                                                    
    pp.description,                                                   
    pp.amount,                                                        
    COALESCE(pp.email, m.email) AS email,                             
    pp.dni,                                                           
    pp.document_id,                                                   
    pp.event,                                                         
    pp.status AS payment_status,                                      
    m.name AS member_name,                                            
    m.last_name AS member_lastname,                                   
    m.member_number AS member_number,                                 
    m.email AS member_email,                                          
    m.phone AS member_phone,                                          
    tp.document_id AS tickets_package_document_id,                    
    tp.final_total AS importe_final_descuentos,                       
    tp.promo_discount_by_payment_method AS promocion_aplicada,        
    tp.payment_method AS metodos_de_pago,                             
    tp.total_tickets_in_package AS cantidad_de_tickets,               
    tp.created_date,                                                  
    tp.admin_user_id,                                                 
    tp.sector_id AS tickets_package_sector_id,                        
    tp.paymentPlanId AS Payments_Relation_ID,                         
    tp.memberId AS Members_Relation_ID,                               
    tp.is_deleted AS tickets_package_is_deleted,                      
    tp.status,                                                        
    tp.event_id                                                       
  FROM                                                                
    `casla-fanhub-admin-prod.firestore_export.PagoticPayments` AS pp  
  LEFT JOIN                                                           
    `casla-fanhub-admin-prod.firestore_export.Members` AS m           
    ON pp.buyerMemberId = m.id                                        
  LEFT JOIN                                                           
    `casla-fanhub-admin-prod.firestore_export.TicketsPackage` AS tp   
    ON tp.memberId = m.id                                             
  WHERE                                                               
    pp.buyerMemberId IS NOT NULL                                      
    AND ARRAY_LENGTH(pp.ticket_ids) > 0                               
    AND pp.status = 'approved';
