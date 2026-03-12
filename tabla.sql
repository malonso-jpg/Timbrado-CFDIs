CREATE SCHEMA IF NOT EXISTS nomina;
CREATE TABLE IF NOT EXISTS nomina.procesos_cfdi (
  id BIGSERIAL PRIMARY KEY,

  -- Identidad lógica del CFDI
  nombre_archivo VARCHAR(255) NOT NULL UNIQUE,
  rfc_emisor VARCHAR(13) NOT NULL,
  rfc_receptor VARCHAR(13),
  folio VARCHAR(50),
  fila_excel INTEGER,

  -- ETAPA 1: Generado
  generado BOOLEAN NOT NULL DEFAULT FALSE,
  fecha_generado TIMESTAMP,
  error_generado TEXT,
  ruta_xml TEXT,

  -- ETAPA 2: Sellado
  sellado BOOLEAN NOT NULL DEFAULT FALSE,
  fecha_sellado TIMESTAMP,
  error_sellado TEXT,
  ruta_sellado TEXT,

  -- ETAPA 3: Timbrado
  timbrado BOOLEAN NOT NULL DEFAULT FALSE,
  fecha_timbrado TIMESTAMP,
  uuid VARCHAR(36),
  codigo_pac VARCHAR(30),
  mensaje_pac TEXT,
  ruta_xml_timbrado TEXT,

  -- Control
  fecha_creacion TIMESTAMP NOT NULL DEFAULT NOW(),
  ultima_actualizacion TIMESTAMP NOT NULL DEFAULT NOW(),
  archivo_origen VARCHAR(500),
  ambiente VARCHAR(10) NOT NULL DEFAULT 'TEST' CHECK (ambiente IN ('TEST', 'PROD')),

  -- cancelacion
  cancelado BOOLEAN NOT NULL DEFAULT FALSE,
  fecha_cancelacion TIMESTAMP,
  codigo_cancelacion VARCHAR(30),
  mensaje_cancelacion TEXT,
  ruta_acuse_cancelacion TEXT

);

ALTER TABLE nomina.procesos_cfdi OWNER TO intelitax;
-- Índices recomendados
CREATE INDEX IF NOT EXISTS idx_procesos_rfc
ON nomina.procesos_cfdi (rfc_receptor);

CREATE INDEX IF NOT EXISTS idx_procesos_timbrado
ON nomina.procesos_cfdi (timbrado);

CREATE INDEX IF NOT EXISTS idx_procesos_uuid
ON nomina.procesos_cfdi (uuid);

CREATE INDEX IF NOT EXISTS idx_procesos_ambiente
ON nomina.procesos_cfdi (ambiente);