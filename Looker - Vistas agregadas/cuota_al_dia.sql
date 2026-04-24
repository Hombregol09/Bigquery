WITH
  UniqueMembers AS (
    SELECT *
    FROM (
      SELECT
        id,
        member_number,
        REGEXP_REPLACE(dni, r'[^0-9]', '') AS dni,
        name,
        last_name,
        email,
        address,
        badges,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY member_number DESC) AS rn
      FROM `casla-fanhub-admin-prod.firestore_export.Members`
      WHERE is_deleted IS NOT TRUE
    )
    WHERE rn = 1
  ),

  -- Mapeo de nombres de insignias con limpieza de comillas
  BadgeNames AS (
    SELECT id, name FROM `casla-fanhub-admin-prod.firestore_export.Badges` 
    WHERE is_deleted IS NOT TRUE
  ),

  -- Consolidado para la tabla (una fila por socio)
  MemberSummary AS (
    SELECT 
      m.id AS member_id,
      STRING_AGG(DISTINCT bg.name, ', ' ORDER BY bg.name) AS insignia_completa
    FROM UniqueMembers m
    CROSS JOIN UNNEST(m.badges) AS badge_id_raw
    JOIN BadgeNames bg ON TRIM(REGEXP_REPLACE(badge_id_raw, r'["\']', '')) = bg.id
    GROUP BY m.id
  )

SELECT
  -- Campo único para métricas de Recuento Distintivo en Looker
  m.id                              AS member_id, 
  m.dni                             AS nro_dni,
  m.member_number                   AS nro_socio,
  CONCAT(m.name, ' ', m.last_name)  AS nombre_apellido,
  m.email                           AS email,
  m.address                         AS direccion,
  
  -- Columna para mostrar en la tabla (una sola fila)
  COALESCE(ms.insignia_completa, 'Sin Insignia') AS insignia_tabla,
  
  -- Columna expandida para FILTROS individuales y GRÁFICOS circulares
  COALESCE(bg_indiv.name, 'Sin Insignia') AS insignia_individual

FROM UniqueMembers m
LEFT JOIN MemberSummary ms ON m.id = ms.member_id
-- Expandimos para permitir el filtro por etiqueta individual
LEFT JOIN UNNEST(m.badges) AS badge_id_raw_2
LEFT JOIN BadgeNames bg_indiv ON TRIM(REGEXP_REPLACE(badge_id_raw_2, r'["\']', '')) = bg_indiv.id

WHERE m.member_number IS NOT NULL OR m.id IS NOT NULL
ORDER BY m.member_number ASC
