# NECT — Kafka Demo: Transacciones Bancarias en Tiempo Real

Demo para el video de YouTube "Kafka desde cero".

## Stack
- **Kafka** (Confluent 7.6) + Zookeeper
- **PostgreSQL 16** — persistencia de transacciones
- **Prometheus** — scraping de métricas
- **Grafana** — dashboard en tiempo real
- **Python** — producer y consumers con Rich + kafka-python

## Caso de uso
Un cliente hace una transacción → el evento entra a Kafka → 3 consumers
en consumer groups **independientes** lo procesan en paralelo:

| Consumer | Group ID | Función |
|---|---|---|
| A | `persistencia-db` | Inserta en PostgreSQL |
| B | `deteccion-fraude` | Detecta fraude → publica en topic `alertas` |
| C | `metricas-grafana` | Actualiza métricas Prometheus |
| Alertas | `log-alertas` | Consume topic `alertas` y loguea |

## Setup

### 1. Levantar infraestructura
```bash
docker-compose up -d
```

Esperar ~30 segundos para que Kafka esté listo.

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

**Terminal 5 — Producer:**
```bash
python producer/producer.py
```

### 4. Ver dashboard
- Grafana: http://localhost:3000 (admin / nect123)
- Dashboard: **NECT Kafka Demo > Transacciones en Tiempo Real**
- Prometheus: http://localhost:9090

## Estructura del proyecto
```
kafka-demo/
├── docker-compose.yml
├── prometheus.yml
├── requirements.txt
├── schema.py                          # Modelo + datos mock chilenos
├── init-scripts/
│   └── init.sql                       # Schema PostgreSQL
├── producer/
│   └── producer.py                    # Genera eventos de transacciones
├── consumers/
│   ├── consumer_a_persistencia.py     # → PostgreSQL
│   ├── consumer_b_fraude.py           # → topic alertas
│   ├── consumer_c_metricas.py         # → Prometheus/Grafana
│   └── consumer_alertas.py           # lee topic alertas
└── grafana/
    ├── datasources/prometheus.yml
    └── dashboards/
        ├── dashboard.yml
        └── nect-kafka.json
```

## Topics Kafka
- `transacciones` — eventos principales
- `alertas` — alertas de fraude generadas por consumer B

## Conexión a KSQL / Kafka Streams (video futuro)
El topic `transacciones` está listo para conectar KSQL:
```sql
CREATE STREAM transacciones (
  evento_id VARCHAR, rut VARCHAR, nombre VARCHAR,
  tipo VARCHAR, monto DOUBLE, region VARCHAR
) WITH (KAFKA_TOPIC='transacciones', VALUE_FORMAT='JSON');
```
