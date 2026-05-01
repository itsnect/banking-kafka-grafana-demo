"""
consumer_alertas.py — Consumer Group: log-alertas
Topic: alertas

Consume las alertas de fraude que publica consumer_b.
En la demo: muestra en tiempo real que el topic 'alertas' es independiente
y que cualquier servicio puede suscribirse a él.
"""

import json
from kafka import KafkaConsumer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────

KAFKA_BROKER = "localhost:9092"
TOPIC        = "alertas"
GROUP_ID     = "log-alertas"

console = Console()


# ─── CONSUMER ─────────────────────────────────────────────────────────────────

def main():
    console.print(Panel(
        f"[bold yellow]Consumer Alertas — Lector del topic alertas[/]\n"
        f"Group: [cyan]{GROUP_ID}[/] | Topic: [cyan]{TOPIC}[/]\n\n"
        "[dim]Este consumer es independiente del que detectó el fraude.\n"
        "Cualquier servicio puede suscribirse al topic 'alertas'.[/]",
        title="🔔 NECT Consumer Alertas",
        border_style="yellow",
    ))

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    console.print("[green]✓ Escuchando alertas...[/]\n")
    contador = 0

    try:
        for mensaje in consumer:
            alerta = mensaje.value
            contador += 1

            tabla = Table(box=box.HEAVY_EDGE, show_header=False, padding=(0, 2))
            tabla.add_column(style="bold yellow", no_wrap=True)
            tabla.add_column(style="white")

            tabla.add_row("N° Alerta",  str(contador))
            tabla.add_row("Cliente",    alerta.get("nombre", "—"))
            tabla.add_row("RUT",        alerta.get("rut", "—"))
            tabla.add_row("Monto",      f"${alerta.get('monto', 0):,.0f} CLP")
            tabla.add_row("Tipo",       alerta.get("tipo", "—"))
            tabla.add_row("Región",     alerta.get("region", "—"))
            tabla.add_row("Motivo",     alerta.get("motivo", "—"))
            tabla.add_row("Timestamp",  alerta.get("timestamp", "—"))
            tabla.add_row("Partición",  str(mensaje.partition))
            tabla.add_row("Offset",     str(mensaje.offset))

            console.print(Panel(
                tabla,
                title=f"[bold red]🚨 ALERTA #{contador} RECIBIDA[/]",
                border_style="red",
            ))
            console.print()

    except KeyboardInterrupt:
        console.print(f"\n[yellow]Consumer Alertas detenido. Total alertas recibidas: {contador}[/]")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
