"""
consumer_b_fraude.py — Consumer Group: deteccion-fraude
Topic: transacciones → analiza → topic: alertas

Detecta transacciones sospechosas y publica alertas en un topic aparte.
Reglas de fraude (simples, para la demo):
  - Monto > $2.000.000 CLP en compra o retiro
  - Monto > $5.000.000 CLP en cualquier tipo
"""

import json
from kafka import KafkaConsumer, KafkaProducer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box
from datetime import datetime, timezone

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from schema import EventoTransaccion

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────

KAFKA_BROKER    = "localhost:9092"
TOPIC_ENTRADA   = "transacciones"
TOPIC_ALERTAS   = "alertas"
GROUP_ID        = "deteccion-fraude"

# Umbrales en CLP
UMBRAL_COMPRA_RETIRO = 2_000_000
UMBRAL_GENERAL       = 5_000_000

console = Console()


# ─── LÓGICA DE FRAUDE ─────────────────────────────────────────────────────────

def evaluar_fraude(evento: EventoTransaccion) -> str | None:
    """Retorna motivo de la alerta o None si no hay fraude."""
    if evento.monto > UMBRAL_GENERAL:
        return f"Monto extremadamente alto: ${evento.monto:,.0f} CLP"

    if evento.tipo in ("compra", "retiro") and evento.monto > UMBRAL_COMPRA_RETIRO:
        return (
            f"{evento.tipo.capitalize()} inusual: "
            f"${evento.monto:,.0f} CLP supera umbral de ${UMBRAL_COMPRA_RETIRO:,}"
        )

    return None


def construir_alerta(evento: EventoTransaccion, motivo: str) -> dict:
    return {
        "evento_id": evento.evento_id,
        "rut":       evento.rut,
        "nombre":    evento.nombre,
        "monto":     evento.monto,
        "tipo":      evento.tipo,
        "region":    evento.region,
        "motivo":    motivo,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ─── CONSUMER + PRODUCER ──────────────────────────────────────────────────────

def main():
    console.print(Panel(
        f"[bold red]Consumer B — Detección de Fraude[/]\n"
        f"Group: [cyan]{GROUP_ID}[/]\n"
        f"Escucha: [cyan]{TOPIC_ENTRADA}[/] → Publica: [cyan]{TOPIC_ALERTAS}[/]",
        title="🚨 NECT Consumer B",
        border_style="red",
    ))

    consumer = KafkaConsumer(
        TOPIC_ENTRADA,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    producer_alertas = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    console.print("[green]✓ Consumer y producer de alertas listos[/]\n")

    try:
        for mensaje in consumer:
            data = mensaje.value
            evento = EventoTransaccion(**data)
            motivo = evaluar_fraude(evento)

            if motivo:
                alerta = construir_alerta(evento, motivo)
                producer_alertas.send(
                    TOPIC_ALERTAS,
                    key=evento.rut,
                    value=alerta,
                )
                producer_alertas.flush()

                tabla = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
                tabla.add_row("[red]cliente[/]", evento.nombre)
                tabla.add_row("[red]rut[/]",     evento.rut)
                tabla.add_row("[red]monto[/]",   f"[bold red]${evento.monto:,.0f} CLP[/]")
                tabla.add_row("[red]tipo[/]",    evento.tipo)
                tabla.add_row("[red]región[/]",  evento.region)
                tabla.add_row("[red]motivo[/]",  motivo)

                console.print(Panel(
                    tabla,
                    title="[bold red]🚨 ALERTA PUBLICADA → alertas[/]",
                    border_style="red",
                ))
            else:
                console.print(
                    f"[dim]✓ Sin fraude — {evento.nombre} "
                    f"${evento.monto:,.0f} CLP ({evento.tipo})[/]"
                )

    except KeyboardInterrupt:
        console.print("\n[yellow]Consumer B detenido.[/]")
    finally:
        consumer.close()
        producer_alertas.close()


if __name__ == "__main__":
    main()
