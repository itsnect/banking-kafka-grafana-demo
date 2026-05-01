"""
consumer_c_metricas.py — Consumer Group: metricas-grafana
Topic: transacciones → métricas Prometheus → Grafana

Expone un endpoint HTTP en :8000/metrics que Prometheus scrapea.
Grafana visualiza en tiempo real.
"""

import json
import threading
from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from rich.console import Console
from rich.panel import Panel

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from schema import EventoTransaccion

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────

KAFKA_BROKER     = "localhost:9092"
TOPIC            = "transacciones"
GROUP_ID         = "metricas-grafana"
METRICS_PORT     = 8000

console = Console()


# ─── MÉTRICAS PROMETHEUS ──────────────────────────────────────────────────────

transacciones_total = Counter(
    "nect_transacciones_total",
    "Total de transacciones procesadas",
    ["tipo", "region"],
)

monto_total = Counter(
    "nect_monto_total_clp",
    "Monto acumulado en CLP",
    ["tipo"],
)

monto_histogram = Histogram(
    "nect_monto_clp",
    "Distribución de montos en CLP",
    buckets=[10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000],
)

transacciones_activas = Gauge(
    "nect_transacciones_ultimo_minuto",
    "Transacciones procesadas (ventana deslizante)",
)

alertas_potenciales = Counter(
    "nect_alertas_fraude_potencial",
    "Transacciones que superan umbral de fraude",
)


# ─── CONSUMER ─────────────────────────────────────────────────────────────────

def procesar_evento(evento: EventoTransaccion) -> None:
    """Actualiza todas las métricas con los datos del evento."""
    transacciones_total.labels(
        tipo=evento.tipo,
        region=evento.region,
    ).inc()

    monto_total.labels(tipo=evento.tipo).inc(evento.monto)
    monto_histogram.observe(evento.monto)
    transacciones_activas.inc()

    if evento.monto > 2_000_000:
        alertas_potenciales.inc()


def consumir():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    console.print("[green]✓ Consumer de métricas listo[/]\n")

    contador = 0
    for mensaje in consumer:
        data = mensaje.value
        evento = EventoTransaccion(**data)
        procesar_evento(evento)
        contador += 1

        if contador % 10 == 0:
            console.print(
                f"[dim cyan]📊 {contador} eventos procesados — "
                f"métricas actualizadas en :8000/metrics[/]"
            )


def main():
    console.print(Panel(
        f"[bold magenta]Consumer C — Métricas Prometheus[/]\n"
        f"Group: [cyan]{GROUP_ID}[/] | Topic: [cyan]{TOPIC}[/]\n"
        f"Métricas expuestas en: [cyan]http://localhost:{METRICS_PORT}/metrics[/]",
        title="📊 NECT Consumer C",
        border_style="magenta",
    ))

    # Levanta servidor de métricas en thread aparte
    start_http_server(METRICS_PORT)
    console.print(f"[green]✓ Servidor Prometheus en :{METRICS_PORT}[/]")

    try:
        consumir()
    except KeyboardInterrupt:
        console.print("\n[yellow]Consumer C detenido.[/]")


if __name__ == "__main__":
    main()
