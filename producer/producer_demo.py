"""
producer_demo.py — Producer interactivo para la demo en cámara
Permite enviar eventos uno a uno, con control total sobre el tipo.

Modos:
  [1] Evento normal    — monto bajo, no activa fraude
  [2] Evento FRAUDE    — monto alto, activa consumer B
  [3] Elegir cliente   — seleccionar cliente específico
  [q] Salir
"""

import json
import sys
import os
from kafka import KafkaProducer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from rich import box

sys.path.insert(0, os.path.dirname(__file__))
from schema import EventoTransaccion, CLIENTES, REGIONES

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────

KAFKA_BROKER = "localhost:9092"
TOPIC        = "transacciones"

console = Console()


# ─── EVENTOS PREDEFINIDOS PARA LA DEMO ────────────────────────────────────────

def evento_normal(cliente_idx: int = 0) -> EventoTransaccion:
    """Transacción normal — no activa fraude."""
    cliente = CLIENTES[cliente_idx]
    return EventoTransaccion(
        rut=cliente["rut"],
        nombre=cliente["nombre"],
        tipo="compra",
        monto=45_990.0,
        comercio="Jumbo",
        region="Metropolitana",
    )


def evento_fraude(cliente_idx: int = 1) -> EventoTransaccion:
    """Transacción sospechosa — monto alto, activa fraude."""
    cliente = CLIENTES[cliente_idx]
    return EventoTransaccion(
        rut=cliente["rut"],
        nombre=cliente["nombre"],
        tipo="retiro",
        monto=7_500_000.0,
        comercio=None,
        region="Antofagasta",
    )


# ─── UI ───────────────────────────────────────────────────────────────────────

def mostrar_menu():
    console.print()
    console.print("[bold white]¿Qué evento quieres enviar?[/]")
    console.print("  [bold green][1][/] Evento normal    — compra $45.990 en Jumbo")
    console.print("  [bold red][2][/] Evento FRAUDE    — retiro $7.500.000 (activa alerta)")
    console.print("  [bold cyan][3][/] Elegir cliente   — seleccionar de la lista")
    console.print("  [bold yellow][q][/] Salir")
    console.print()


def mostrar_clientes():
    tabla = Table(title="Clientes disponibles", box=box.SIMPLE, show_lines=True)
    tabla.add_column("#", style="cyan", width=4)
    tabla.add_column("Nombre", style="white")
    tabla.add_column("RUT", style="dim")

    for i, c in enumerate(CLIENTES):
        tabla.add_row(str(i), c["nombre"], c["rut"])

    console.print(tabla)


def mostrar_evento_enviado(evento: EventoTransaccion, partition: int, offset: int):
    es_fraude = evento.monto > 2_000_000
    color = "red" if es_fraude else "green"
    icono = "🚨 FRAUDE" if es_fraude else "✅ NORMAL"

    tabla = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
    tabla.add_row("[bold cyan]evento_id[/]",  evento.evento_id[:8] + "...")
    tabla.add_row("[bold cyan]cliente[/]",    evento.nombre)
    tabla.add_row("[bold cyan]rut[/]",        evento.rut)
    tabla.add_row("[bold cyan]tipo[/]",       evento.tipo)
    tabla.add_row(
        "[bold cyan]monto[/]",
        f"[bold {color}]${evento.monto:,.0f} CLP[/]"
    )
    tabla.add_row("[bold cyan]comercio[/]",   evento.comercio or "—")
    tabla.add_row("[bold cyan]región[/]",     evento.region)
    tabla.add_row("[bold cyan]partición[/]",  str(partition))
    tabla.add_row("[bold cyan]offset[/]",     str(offset))

    console.print(Panel(
        tabla,
        title=f"[bold {color}]📤 EVENTO PUBLICADO — {icono}[/]",
        border_style=color,
        subtitle="[dim]Esperando en consumers...[/]",
    ))

    if es_fraude:
        console.print(Panel(
            "[bold red]⚠️  Este evento supera el umbral de fraude.\n"
            "Consumer B va a detectarlo y publicar en el topic [cyan]alertas[/].\n"
            "Consumer Alertas lo va a mostrar en su terminal.[/]",
            border_style="red",
        ))
    else:
        console.print(Panel(
            "[green]✓ Monto normal.\n"
            "Consumer A lo guardará en PostgreSQL.\n"
            "Consumer B lo revisará y lo dejará pasar.\n"
            "Consumer C actualizará las métricas en Grafana.[/]",
            border_style="green",
        ))


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    console.print(Panel(
        "[bold green]Producer Interactivo — Modo Demo[/]\n"
        f"Broker: [cyan]{KAFKA_BROKER}[/] | Topic: [cyan]{TOPIC}[/]\n\n"
        "[dim]Cada evento que envíes llegará a los 3 consumers en paralelo.[/]",
        title="🎬 NECT Kafka — Demo en Cámara",
        border_style="green",
    ))

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    console.print("[green]✓ Conectado a Kafka[/]")

    cliente_seleccionado = 0  # default: Valentina Rojas

    while True:
        mostrar_menu()
        opcion = Prompt.ask(
            "[bold white]Opción[/]",
            choices=["1", "2", "3", "q"],
            default="1",
        )

        if opcion == "q":
            console.print("\n[yellow]Demo finalizada.[/]")
            break

        elif opcion == "3":
            mostrar_clientes()
            idx = Prompt.ask(
                "Número de cliente",
                choices=[str(i) for i in range(len(CLIENTES))],
                default="0",
            )
            cliente_seleccionado = int(idx)
            console.print(
                f"[green]✓ Cliente seleccionado: "
                f"{CLIENTES[cliente_seleccionado]['nombre']}[/]"
            )
            continue  # vuelve al menú sin enviar

        elif opcion == "1":
            evento = evento_normal(cliente_seleccionado)
        elif opcion == "2":
            evento = evento_fraude(cliente_seleccionado)

        # Enviar
        future = producer.send(
            TOPIC,
            key=evento.rut,
            value=evento.model_dump(),
        )
        metadata = future.get(timeout=10)
        producer.flush()

        mostrar_evento_enviado(evento, metadata.partition, metadata.offset)

    producer.close()


if __name__ == "__main__":
    main()
