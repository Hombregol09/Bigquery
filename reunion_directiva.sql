WITH SociosByID AS (
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
SociosByDNI AS (
  SELECT 
    dni, 
    MAX(name) AS name, 
    MAX(last_name) AS last_name, 
    MAX(phone) AS phone, 
    MAX(email) AS email, 
    MAX(member_number) AS member_number
  FROM `casla-fanhub-admin-prod.firestore_export.Members`
  WHERE dni IS NOT NULL AND TRIM(dni) != ''
  GROUP BY dni
),
EventosDirectiva AS (
  SELECT event_id, MAX(description) AS event_description
  FROM `casla-fanhub-admin-prod.firestore_export.Events`
  WHERE LOWER(description) LIKE '%directiva%'
  GROUP BY event_id
)

SELECT 
  t.document_id AS ticket_id,
  COALESCE(t.buyer_dni, sid.dni, sdni.dni) AS dni,
  COALESCE(sid.member_number, t.buyer_member_id, sdni.member_number) AS numero_socio,
  COALESCE(sid.name, sdni.name) AS nombre,
  COALESCE(sid.last_name, sdni.last_name) AS apellido,
  COALESCE(sid.phone, sdni.phone) AS telefono,
  COALESCE(t.buyer_email, sid.email, sdni.email) AS email,
  t.status AS estado_reserva,
  t.category AS categoria,
  t.purchase_date_ddmmyyyy AS fecha_compra,
  e.event_description AS descripcion_evento
FROM `casla-fanhub-admin-prod.firestore_export.Tickets` t
INNER JOIN EventosDirectiva e 
  ON t.event_id = e.event_id
LEFT JOIN SociosByID sid 
  ON (t.member_id = sid.id OR t.buyer_member_id = sid.id)
LEFT JOIN SociosByDNI sdni
  ON (t.buyer_dni = sdni.dni AND sid.id IS NULL)
WHERE t.is_deleted = FALSE
  AND (t.is_ticket_free IS NULL OR t.is_ticket_free = FALSE)
ORDER BY t.purchase_date_timestamp ASC;
