-- ============================================================================
-- Tabla: stream_raw_changelog
-- Dataset: firestore_export
-- Proyecto: casla-fanhub-admin-prod
-- Descripción: Tabla base que contiene el historial de cambios (changelog) de Firestore.
-- NOTA: Esta es una TABLA, no una VISTA. A continuación se detalla su esquema.
-- ============================================================================

/*
ESQUEMA:
[
  {"name":"timestamp","type":"TIMESTAMP","mode":"REQUIRED","description":"The commit timestamp of this change in Cloud Firestore..."},
  {"name":"event_id","type":"STRING","mode":"REQUIRED","description":"The ID of the document change event..."},
  {"name":"document_name","type":"STRING","mode":"REQUIRED","description":"The full name of the changed document..."},
  {"name":"operation","type":"STRING","mode":"REQUIRED","description":"One of CREATE, UPDATE, IMPORT, or DELETE."},
  {"name":"data","type":"STRING","mode":"NULLABLE","description":"The full JSON representation of the document state..."},
  {"name":"old_data","type":"STRING","mode":"NULLABLE","description":"The full JSON representation of the document state..."},
  {"name":"document_id","type":"STRING","mode":"NULLABLE","description":"The document id as defined in the firestore database."},
  {"name":"path_params","type":"STRING","mode":"NULLABLE","description":"JSON string representing wildcard params with Firestore Document ids"}
]
*/
