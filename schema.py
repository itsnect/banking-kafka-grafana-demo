"""
schema.py — Modelo del evento de transacción
Compartido entre producer y consumers.
"""

import uuid
import random
from datetime import datetime, timezone
from pydantic import BaseModel, Field


# ─── MODELO DEL EVENTO ────────────────────────────────────────────────────────

class EventoTransaccion(BaseModel):
    evento_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    rut: str
    nombre: str
    tipo: str                    # transferencia | pago | compra | retiro
    monto: float
    moneda: str = "CLP"
    comercio: str | None = None
    region: str
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


# ─── DATOS MOCK CHILENOS ──────────────────────────────────────────────────────

CLIENTES = [
    {"rut": "12.345.678-9", "nombre": "Valentina Rojas Fuentes"},
    {"rut": "15.678.901-2", "nombre": "Matías Contreras Pizarro"},
    {"rut": "11.222.333-4", "nombre": "Camila Soto Herrera"},
    {"rut": "16.987.654-3", "nombre": "Diego Muñoz Araya"},
    {"rut": "13.456.789-0", "nombre": "Javiera Espinoza Lagos"},
    {"rut": "14.321.654-K", "nombre": "Sebastián Vidal Morales"},
    {"rut": "17.654.321-5", "nombre": "Constanza Medina Riquelme"},
    {"rut": "10.987.654-6", "nombre": "Nicolás Fuentes Bravo"},
]

TIPOS_TRANSACCION = ["transferencia", "pago", "compra", "retiro"]

COMERCIOS = [
    "Falabella", "Ripley", "Líder", "Jumbo", "Paris",
    "Entel", "Movistar", "Netflix Chile", "Uber Chile",
    "Shell Estación", "Copec", "Farmacia Cruz Verde",
    "McDonald's Chile", "Subway", "Starbucks Las Condes",
    None, None,  # transferencias no tienen comercio
]

REGIONES = [
    "Metropolitana", "Valparaíso", "Biobío",
    "La Araucanía", "Los Lagos", "Antofagasta",
    "Coquimbo", "O'Higgins",
]

# Rangos de monto por tipo (en CLP)
RANGOS_MONTO = {
    "transferencia": (50_000, 5_000_000),
    "pago":          (5_000, 500_000),
    "compra":        (2_990, 299_990),
    "retiro":        (10_000, 200_000),
}


def generar_transaccion() -> EventoTransaccion:
    """Genera una transacción aleatoria con datos chilenos."""
    cliente = random.choice(CLIENTES)
    tipo = random.choice(TIPOS_TRANSACCION)
    monto_min, monto_max = RANGOS_MONTO[tipo]

    # Simula ocasionalmente montos sospechosos (fraude)
    if random.random() < 0.08:  # 8% de probabilidad
        monto = random.uniform(3_000_000, 9_500_000)
    else:
        monto = random.uniform(monto_min, monto_max)

    return EventoTransaccion(
        rut=cliente["rut"],
        nombre=cliente["nombre"],
        tipo=tipo,
        monto=round(monto, 2),
        comercio=random.choice(COMERCIOS) if tipo != "transferencia" else None,
        region=random.choice(REGIONES),
    )
