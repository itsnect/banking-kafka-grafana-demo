"""
producer.py — Produce eventos de transacciones a Kafka
Topic: transacciones

Simula clientes realizando operaciones bancarias en tiempo real.
"""

import json
import time
import random
from kafka import KafkaProducer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from schema import generar_transaccion

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────

KAFKA_BROKER = "localhost:9092"
TOPIC        = "transacciones"
INTERVALO_SEG = 1.5   # pausa entre eventos

console = Console()


# ─── PRODUCER ─────────────────────────────────────────────────────────────────

def crear_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # La key es el RUT — garantiza orden por cliente en la misma partición
        key_serializer=lambda k: k.encode("utf-8"),
    )


def publicar_evento(producer: KafkaProducer, evento) -> None:
    future = producer.send(
        TOPIC,
        key=evento.rut,
        value=evento.model_dump(),
    )
    metadata = future.get(timeout=10)

    # Log visual en consola
    tabla = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
    tabla.add_row("[bold cyan]evento_id[/]", evento.evento_id[:8] + "...")
    tabla.add_row("[bold cyan]cliente[/]",   evento.nombre)
    tabla.add_row("[bold cyan]tipo[/]",      evento.tipo)
    tabla.add_row(
        "[bold cyan]monto[/]",
        f"[bold yellow]${evento.monto:,.0f} CLP[/]"
    )
    tabla.add_row("[bold cyan]región[/]",    evento.region)
    tabla.add_row("[bold cyan]partición[/]", str(metadata.partition))
    tabla.add_row("[bold cyan]offset[/]",    str(metadata.offset))

    color = "red" if evento.monto > 2_000_000 else "green"
    console.print(Panel(
        tabla,
        title=f"[bold {color}]📤 EVENTO PUBLICADO[/]",
        border_style=color,
    ))


def main():
    console.print(Panel(
        "[bold green]Iniciando producer de transacciones...[/]\n"
        f"Broker: [cyan]{KAFKA_BROKER}[/] | Topic: [cyan]{TOPIC}[/]",
        title="🚀 NECT Kafka Producer",
        border_style="green",
    ))

    producer = crear_producer()
    contador = 0

    try:
        while True:
            evento = generar_transaccion()
            publicar_evento(producer, evento)
            contador += 1
            console.print(f"[dim]Total publicados: {contador}[/]\n")
            time.sleep(random.uniform(0.8, INTERVALO_SEG))

    except KeyboardInterrupt:
        console.print("\n[yellow]Producer detenido.[/]")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
