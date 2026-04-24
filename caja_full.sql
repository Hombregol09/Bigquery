-- ============================================
-- QUERY UNIFICADA: VENTAS SEDE + WEB
-- Club Atlético San Lorenzo de Almagro
-- ============================================

WITH
  -- =====================
  -- VENTAS EN SEDE
  -- =====================
  TicketsPackage_Cleaned AS (
    SELECT
      document_id AS tickets_package_document_id,
      final_total AS importe_final_descuentos,
      promo_discount_by_payment_method AS promocion_aplicada,
      payment_method AS metodo_de_pago,
      total_tickets_in_package AS cantidad_de_tickets,
      created_date,
      admin_user_id,
      sector_id AS tickets_package_sector_id,
      paymentPlanId AS Payments_Relation_ID,
      memberId AS Members_Relation_ID,
      is_deleted AS tickets_package_is_deleted,
      status,
      event_id,
      isRenewal,
      partialPaymentDetails
    FROM `casla-fanhub-admin-prod.firestore_export.TicketsPackage`
  ),
  UniqueTicketsPackage AS (
    SELECT
      tickets_package_document_id,
      importe_final_descuentos,
      promocion_aplicada,
      metodo_de_pago,
      cantidad_de_tickets,
      created_date,
      admin_user_id,
      tickets_package_sector_id,
      Payments_Relation_ID,
      Members_Relation_ID,
      tickets_package_is_deleted,
      status,
      event_id,
      isRenewal,
      partialPaymentDetails
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY tickets_package_document_id ORDER BY created_date DESC) as rn
      FROM TicketsPackage_Cleaned
    )
    WHERE rn = 1
  ),
  -- APLICAMOS "MAX" EN LUGAR DE "ANY_VALUE" PARA IGNORAR LOS NULOS (así no se pierden los nombres como pasa con ANY_VALUE)
  UniqueUsers AS (
    SELECT id, MAX(name) AS name, MAX(email) AS email
    FROM `casla-fanhub-admin-prod.firestore_export.Users`
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
    SELECT id, MAX(name) AS name
    FROM `casla-fanhub-admin-prod.firestore_export.Payments`
    GROUP BY id
  ),
  UniqueMembers AS (
    SELECT 
      id, 
      MAX(name) AS name, 
      MAX(last_name) AS last_name, 
      MAX(phone) AS phone, 
      MAX(email) AS email, 
      MAX(dni) AS dni, 
      MAX(member_number) AS member_number
    FROM `casla-fanhub-admin-prod.firestore_export.Members`
    GROUP BY id
  ),
  UniqueEvents AS (
    SELECT event_id, MAX(description) AS event_description
    FROM `casla-fanhub-admin-prod.firestore_export.Events`
    GROUP BY event_id
  ),
  
  -- =====================
  -- QUERY SEDE
  -- =====================
  VentasSede AS (
    SELECT
      'SEDE' AS canal_venta,
      tp.tickets_package_document_id AS document_id,
      FORMAT_DATE('%Y-%m-%d', tp.created_date) AS fecha_venta,
      CAST(tp.event_id AS STRING) AS event_id,
      e.event_description,
      
      -- REGLA DE NEGOCIO RESTAURADA DESDE TU DOCUMENTO
      CASE                                                                                                   
        WHEN tp.event_id = '4e811ae3-948a-4079-b64d-84b48a64a404' THEN                                       
          CASE                                                                                               
            WHEN tp.isRenewal = TRUE THEN 'Renovación Abono - Pando 2025/2026'                               
            ELSE 'Nuevo Abono - Pando 2025/2026'                                                             
          END                                                                                                
        ELSE NULL                                                                                            
      END AS origen_compra,
      
      tp.importe_final_descuentos AS monto_total,
      tp.cantidad_de_tickets,
      tp.promocion_aplicada,
      tp.metodo_de_pago,
      p.name AS plan_de_pagos,
      u.name AS usuario_vendedor,
      u.email AS email_vendedor,
      pl.sector_name AS sector_estadio,
      pl.sector_code AS codigo_sector,
      pl.name_location,
      pl.adress_location,
      COALESCE(m.email, '') AS email_cliente,
      m.dni AS dni_cliente,
      m.name AS nombre_socio,
      m.last_name AS apellido_socio,
      m.phone AS telefono_socio,
      m.member_number AS numero_socio,
      tp.tickets_package_is_deleted AS registro_eliminado,
      tp.status AS estado_venta,
      CAST(NULL AS STRING) AS pagotic_id,
      CAST(NULL AS STRING) AS brand,
      CAST(NULL AS STRING) AS installments,
      CAST(NULL AS STRING) AS installment_amount,
      tp.partialPaymentDetails,

      -- COLUMNAS EXCLUSIVAS PARA CONTADURIA: Calculan la distribución del pago preservando 1 fila = 1 Venta
      CASE 
        WHEN tp.metodo_de_pago = 'Parcial' AND tp.partialPaymentDetails IS NOT NULL THEN
          COALESCE((SELECT SUM(CAST(JSON_VALUE(p, '$.amount') AS FLOAT64)) FROM UNNEST(JSON_EXTRACT_ARRAY(tp.partialPaymentDetails, '$')) p WHERE LOWER(JSON_VALUE(p, '$.method')) LIKE '%tarjeta%'), 0)
        WHEN LOWER(tp.metodo_de_pago) LIKE '%tarjeta%' THEN tp.importe_final_descuentos
        ELSE 0 
      END AS monto_tarjetas,

      CASE 
        WHEN tp.metodo_de_pago = 'Parcial' AND tp.partialPaymentDetails IS NOT NULL THEN
          COALESCE((SELECT SUM(CAST(JSON_VALUE(p, '$.amount') AS FLOAT64)) FROM UNNEST(JSON_EXTRACT_ARRAY(tp.partialPaymentDetails, '$')) p WHERE LOWER(JSON_VALUE(p, '$.method')) LIKE '%transferencia%' OR LOWER(JSON_VALUE(p, '$.method')) LIKE '%qr%'), 0)
        WHEN LOWER(tp.metodo_de_pago) LIKE '%transferencia%' OR LOWER(tp.metodo_de_pago) LIKE '%qr%' THEN tp.importe_final_descuentos
        ELSE 0 
      END AS monto_transferencias,

      CASE 
        WHEN tp.metodo_de_pago = 'Parcial' AND tp.partialPaymentDetails IS NOT NULL THEN
          COALESCE((SELECT SUM(CAST(JSON_VALUE(p, '$.amount') AS FLOAT64)) FROM UNNEST(JSON_EXTRACT_ARRAY(tp.partialPaymentDetails, '$')) p WHERE LOWER(JSON_VALUE(p, '$.method')) LIKE '%efectivo%'), 0)
        WHEN LOWER(tp.metodo_de_pago) LIKE '%efectivo%' THEN tp.importe_final_descuentos
        ELSE 0 
      END AS monto_efectivo,

      CASE 
        WHEN tp.metodo_de_pago = 'Parcial' AND tp.partialPaymentDetails IS NOT NULL THEN
          COALESCE((SELECT SUM(CAST(JSON_VALUE(p, '$.amount') AS FLOAT64)) FROM UNNEST(JSON_EXTRACT_ARRAY(tp.partialPaymentDetails, '$')) p WHERE LOWER(JSON_VALUE(p, '$.method')) NOT LIKE '%tarjeta%' AND LOWER(JSON_VALUE(p, '$.method')) NOT LIKE '%transferencia%' AND LOWER(JSON_VALUE(p, '$.method')) NOT LIKE '%qr%' AND LOWER(JSON_VALUE(p, '$.method')) NOT LIKE '%efectivo%'), 0)
        WHEN tp.metodo_de_pago = 'Parcial' AND tp.partialPaymentDetails IS NULL THEN tp.importe_final_descuentos
        WHEN LOWER(tp.metodo_de_pago) NOT LIKE '%tarjeta%' AND LOWER(tp.metodo_de_pago) NOT LIKE '%transferencia%' AND LOWER(tp.metodo_de_pago) NOT LIKE '%qr%' AND LOWER(tp.metodo_de_pago) NOT LIKE '%efectivo%' THEN tp.importe_final_descuentos
        ELSE 0 
      END AS monto_otros
      
    FROM UniqueTicketsPackage AS tp
    LEFT JOIN UniqueUsers AS u ON tp.admin_user_id = u.id
    LEFT JOIN UniquePlaces AS pl ON tp.tickets_package_sector_id = pl.sector_id
    LEFT JOIN UniquePayments AS p ON tp.Payments_Relation_ID = p.id
    LEFT JOIN UniqueMembers AS m ON tp.Members_Relation_ID = m.id
    LEFT JOIN UniqueEvents AS e ON tp.event_id = e.event_id
    WHERE UPPER(tp.status) = 'APPROVED'
      AND tp.tickets_package_is_deleted != TRUE
  ),
  
  -- =====================
  -- QUERY WEB
  -- =====================
  VentasWeb AS (
    SELECT DISTINCT
      'WEB' AS canal_venta,
      pp.document_id,
      FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y/%m/%d', pp.updated_at_ddmmyyyy)) AS fecha_venta,
      CAST(pp.event AS STRING) AS event_id,
      e.event_description,
      
      -- REGLA DE NEGOCIO RESTAURADA DE ORIGEN COMPRA WEB
      CASE                                                                                                   
         WHEN pp.description = 'Compra desde la app' THEN 'Nuevo Abono - Pando 2025/2026'                    
         WHEN pp.description = 'Renovación de abono desde la app' THEN 'Renovación Abono - Pando 2025/2026'  
         ELSE NULL                                                                                           
      END AS origen_compra,
      
      pp.amount AS monto_total,
      pp.tickets_count AS cantidad_de_tickets,
      CAST(NULL AS STRING) AS promocion_aplicada,
      CASE                                                                                                   
        WHEN pp.payment_type = 'CREDITCARD' THEN 'Tarjeta de crédito Web'                                    
        WHEN pp.payment_type IS NULL THEN 'Tarjeta de crédito Web'                                           
        WHEN pp.payment_type = 'DEBITCARD' THEN 'Tarjeta de débito Web'                                      
        ELSE pp.payment_type                                                                                 
      END AS metodo_de_pago,
      p.name AS plan_de_pagos,
      'Autogestión' AS usuario_vendedor,
      CAST(NULL AS STRING) AS email_vendedor,
      pl.sector_name AS sector_estadio,
      pl.sector_code AS codigo_sector,
      pl.name_location,
      pl.adress_location,
      COALESCE(pp.email, m.email, '') AS email_cliente,
      pp.dni AS dni_cliente,
      m.name AS nombre_socio,
      m.last_name AS apellido_socio,
      m.phone AS telefono_socio,
      m.member_number AS numero_socio,
      FALSE AS registro_eliminado,
      pp.status AS estado_venta,
      pp.pagotic_id,
      pp.brand,
      pp.installments,
      pp.installment_amount,
      CAST(NULL AS STRING) AS partialPaymentDetails,

      -- COLUMNAS EXCLUSIVAS PARA CONTADURIA: Calculan la distribución del pago preservando 1 fila = 1 Venta
      CASE 
        WHEN pp.payment_type IN ('CREDITCARD', 'DEBITCARD') OR pp.payment_type IS NULL THEN pp.amount
        WHEN LOWER(pp.payment_type) LIKE '%tarjeta%' THEN pp.amount
        ELSE 0 
      END AS monto_tarjetas,

      CASE 
        WHEN pp.payment_type NOT IN ('CREDITCARD', 'DEBITCARD') AND pp.payment_type IS NOT NULL AND (LOWER(pp.payment_type) LIKE '%transferencia%' OR LOWER(pp.payment_type) LIKE '%qr%') THEN pp.amount
        ELSE 0 
      END AS monto_transferencias,

      CASE 
        WHEN pp.payment_type NOT IN ('CREDITCARD', 'DEBITCARD') AND pp.payment_type IS NOT NULL AND LOWER(pp.payment_type) LIKE '%efectivo%' THEN pp.amount
        ELSE 0 
      END AS monto_efectivo,

      CASE 
        WHEN pp.payment_type NOT IN ('CREDITCARD', 'DEBITCARD') AND pp.payment_type IS NOT NULL AND LOWER(pp.payment_type) NOT LIKE '%tarjeta%' AND LOWER(pp.payment_type) NOT LIKE '%transferencia%' AND LOWER(pp.payment_type) NOT LIKE '%qr%' AND LOWER(pp.payment_type) NOT LIKE '%efectivo%' THEN pp.amount
        ELSE 0 
      END AS monto_otros
      
    FROM `casla-fanhub-admin-prod.firestore_export.PagoticPayments` AS pp
    LEFT JOIN UniqueMembers AS m ON pp.buyerMemberId = m.id
    LEFT JOIN UniquePayments AS p ON pp.payment_plan_id = p.id
    LEFT JOIN UniquePlaces AS pl ON pp.sector_id = pl.sector_id
    LEFT JOIN UniqueEvents AS e ON pp.event = e.event_id
    WHERE pp.buyerMemberId IS NOT NULL
      AND pp.status = 'approved'
  )

-- =====================
-- UNIÓN FINAL
-- =====================
SELECT * FROM VentasSede
UNION ALL
SELECT * FROM VentasWeb
ORDER BY fecha_venta DESC, canal_venta;
