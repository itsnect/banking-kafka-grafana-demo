"""
consumer_a_persistencia.py — Consumer Group: persistencia-db
Topic: transacciones → PostgreSQL

Cada evento que llega se persiste en la tabla transacciones.
"""

import json
import psycopg2
from kafka import KafkaConsumer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from schema import EventoTransaccion

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────

KAFKA_BROKER   = "localhost:9092"
TOPIC          = "transacciones"
GROUP_ID       = "persistencia-db"          # consumer group independiente

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "transacciones_db",
    "user":     "nect",
    "password": "nect123",
}

console = Console()


# ─── PERSISTENCIA ─────────────────────────────────────────────────────────────

def insertar_transaccion(conn, evento: EventoTransaccion) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO transacciones
                (evento_id, rut, nombre, tipo, monto, moneda, comercio, region, timestamp)
            VALUES
                (%(evento_id)s, %(rut)s, %(nombre)s, %(tipo)s, %(monto)s,
                 %(moneda)s, %(comercio)s, %(region)s, %(timestamp)s)
            ON CONFLICT (evento_id) DO NOTHING
        """, evento.model_dump())
    conn.commit()


# ─── CONSUMER ─────────────────────────────────────────────────────────────────

def main():
    console.print(Panel(
        f"[bold blue]Consumer A — Persistencia PostgreSQL[/]\n"
        f"Group: [cyan]{GROUP_ID}[/] | Topic: [cyan]{TOPIC}[/]",
        title="🗄️  NECT Consumer A",
        border_style="blue",
    ))

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    conn = psycopg2.connect(**DB_CONFIG)
    console.print("[green]✓ Conectado a PostgreSQL[/]\n")

    try:
        for mensaje in consumer:
            data = mensaje.value
            evento = EventoTransaccion(**data)

            insertar_transaccion(conn, evento)

            tabla = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
            tabla.add_row("[cyan]cliente[/]",   evento.nombre)
            tabla.add_row("[cyan]tipo[/]",      evento.tipo)
            tabla.add_row("[cyan]monto[/]",     f"${evento.monto:,.0f} CLP")
            tabla.add_row("[cyan]partición[/]", str(mensaje.partition))
            tabla.add_row("[cyan]offset[/]",    str(mensaje.offset))

            console.print(Panel(
                tabla,
                title="[bold blue]✅ GUARDADO EN POSTGRES[/]",
                border_style="blue",
            ))

    except KeyboardInterrupt:
        console.print("\n[yellow]Consumer A detenido.[/]")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
