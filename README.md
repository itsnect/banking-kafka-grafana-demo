# NECT — Kafka Demo: Transacciones Bancarias en Tiempo Real

Demo para el video de YouTube **"Kafka desde cero"** y **"ksqlDB desde cero"**.

## Stack

- **Kafka** (Confluent 7.6) + Zookeeper
- **PostgreSQL 16** — persistencia de transacciones
- **Prometheus** — scraping de métricas
- **Grafana** — dashboard en tiempo real
- **ksqlDB** — queries SQL sobre streams en tiempo real
- **ksqlDB UI** — interfaz web para ksqlDB
- **Python** — producer y consumers con Rich + kafka-python

## Caso de uso

Un cliente hace una transacción → el evento entra a Kafka → 3 consumer groups lo procesan en paralelo:

| Consumer | Group ID | Función |
|---|---|---|
| A | `persistencia-db` | Inserta en PostgreSQL |
| B | `deteccion-fraude` | Detecta fraude → publica en topic `alertas` |
| C | `metricas-grafana` | Actualiza métricas Prometheus |
| Alertas | `log-alertas` | Consume topic `alertas` y loguea |

Además, **ksqlDB** se conecta al mismo topic `transacciones` y permite hacer queries SQL en tiempo real sin tocar el código existente.

---

## Setup

### 1. Levantar infraestructura

```bash
docker compose up -d
```

Esperar ~30 segundos para que Kafka esté listo. Verificar con:

```bash
docker compose ps
```

Deben aparecer 9 servicios en estado `Up`: zookeeper, kafka, postgres, prometheus, grafana, ksqldb-server, ksqldb-cli, ksqldb-ui.

### 2. Instalar dependencias Python

```bash
pip install -r requirements.txt
```

### 3. Orden de ejecución (terminales separadas)

**Terminal 1 — Consumer A (PostgreSQL):**
```bash
python consumers/consumer_a_persistencia.py
```

**Terminal 2 — Consumer B (Fraude):**
```bash
python consumers/consumer_b_fraude.py
```

**Terminal 3 — Consumer C (Métricas):**
```bash
python consumers/consumer_c_metricas.py
```

**Terminal 4 — Consumer Alertas:**
```bash
python consumers/consumer_alertas.py
```

**Terminal 5 — Producer interactivo (demo) o Producer automático:**
```bash
# Demo manual — evento por evento
python producer/producer_demo.py

# O producer automático en loop
python producer/producer.py
```

### 4. Ver dashboards

| Servicio | URL | Credenciales |
|---|---|---|
| Grafana | http://localhost:3000 | admin / nect123 |
| Prometheus | http://localhost:9090 | — |
| ksqlDB UI | http://localhost:8080 | — |

En Grafana: ** Kafka Demo → Transacciones en Tiempo Real**

---

## ksqlDB — Queries SQL sobre Kafka

### Entrar a la CLI

```bash
docker exec -it kafka-demo-ksqldb-cli-1 ksql-connect.sh http://kafka-demo-ksqldb-server-1:8088
```

### Crear el stream sobre el topic existente

```sql
CREATE STREAM transacciones (
  evento_id VARCHAR,
  rut       VARCHAR,
  nombre    VARCHAR,
  tipo      VARCHAR,
  monto     DOUBLE,
  region    VARCHAR
) WITH (
  KAFKA_TOPIC='transacciones',
  VALUE_FORMAT='JSON'
);
```

### Queries útiles

```sql
-- Ver todos los streams
SHOW STREAMS;

-- Ver todos los topics
SHOW TOPICS;

-- Filtrar transacciones sospechosas en tiempo real
SELECT nombre, tipo, monto, region
FROM transacciones
WHERE monto > 2000000
EMIT CHANGES;

-- Conteo por tipo cada 30 segundos
SELECT tipo,
       COUNT(*) AS total,
       SUM(monto) AS monto_total
FROM transacciones
WINDOW TUMBLING (SIZE 30 SECONDS)
GROUP BY tipo
EMIT CHANGES;

-- Stream derivado persistente — crea nuevo topic en Kafka
CREATE STREAM alertas_ksql AS
  SELECT evento_id, nombre, rut, monto, region,
         'MONTO_ALTO' AS motivo
  FROM transacciones
  WHERE monto > 2000000
EMIT CHANGES;
```

### ksqlDB UI

Abrir `http://localhost:8080` — interfaz web para ejecutar queries sin usar la terminal.

---

## Estructura del proyecto

```
kafka-demo/
├── docker-compose.yml
├── prometheus.yml
├── requirements.txt
├── development.toml               # Config de ksqlDB UI
├── schema.py                      # Modelo + datos mock chilenos
├── init-scripts/
│   └── init.sql                   # Schema PostgreSQL
├── producer/
│   ├── producer.py                # Genera eventos en loop automático
│   └── producer_demo.py           # Producer interactivo (demo en cámara)
├── consumers/
│   ├── consumer_a_persistencia.py # → PostgreSQL
│   ├── consumer_b_fraude.py       # → topic alertas
│   ├── consumer_c_metricas.py     # → Prometheus/Grafana
│   └── consumer_alertas.py        # lee topic alertas
└── grafana/
    ├── datasources/prometheus.yml
    └── dashboards/
        ├── dashboard.yml
        └── nect-kafka.json
```

## Topics Kafka

| Topic | Descripción |
|---|---|
| `transacciones` | Eventos principales del producer |
| `alertas` | Alertas de fraude generadas por consumer B |
| `alertas_ksql` | Stream derivado de ksqlDB (si se crea) |