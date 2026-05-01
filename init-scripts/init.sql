-- Tabla principal de transacciones
CREATE TABLE IF NOT EXISTS transacciones (
    id          SERIAL PRIMARY KEY,
    evento_id   VARCHAR(36)    NOT NULL UNIQUE,
    rut         VARCHAR(12)    NOT NULL,
    nombre      VARCHAR(100)   NOT NULL,
    tipo        VARCHAR(30)    NOT NULL,
    monto       NUMERIC(14, 2) NOT NULL,
    moneda      VARCHAR(5)     DEFAULT 'CLP',
    comercio    VARCHAR(100),
    region      VARCHAR(50),
    timestamp   TIMESTAMPTZ    NOT NULL,
    creado_en   TIMESTAMPTZ    DEFAULT NOW()
);

-- Tabla de alertas de fraude
CREATE TABLE IF NOT EXISTS alertas_fraude (
    id          SERIAL PRIMARY KEY,
    evento_id   VARCHAR(36)    NOT NULL,
    rut         VARCHAR(12)    NOT NULL,
    monto       NUMERIC(14, 2) NOT NULL,
    motivo      VARCHAR(200)   NOT NULL,
    timestamp   TIMESTAMPTZ    NOT NULL,
    creado_en   TIMESTAMPTZ    DEFAULT NOW()
);

-- Índice para consultas por rut
CREATE INDEX IF NOT EXISTS idx_transacciones_rut ON transacciones(rut);
CREATE INDEX IF NOT EXISTS idx_alertas_rut ON alertas_fraude(rut);
