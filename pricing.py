from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

GST_RATE = 0.18


@dataclass(frozen=True)
class PriceBreakdown:
    subtotal: float
    tax: float
    total: float


def compute_gst(subtotal: float) -> PriceBreakdown:
    subtotal = float(subtotal or 0)
    tax = round(subtotal * GST_RATE, 2)
    total = round(subtotal + tax, 2)
    return PriceBreakdown(subtotal=subtotal, tax=tax, total=total)


def hotel_total(price_per_night: float, nights: int, rooms: int = 1) -> PriceBreakdown:
    nights = max(1, int(nights or 1))
    rooms = max(1, int(rooms or 1))
    subtotal = float(price_per_night or 0) * nights * rooms
    return compute_gst(subtotal)


def car_total(base_price_per_day: float, days: int = 1) -> PriceBreakdown:
    days = max(1, int(days or 1))
    subtotal = float(base_price_per_day or 0) * days
    return compute_gst(subtotal)


def courier_total(price_per_kg: float, weight_kg: float) -> PriceBreakdown:
    weight_kg = max(0.1, float(weight_kg or 0.1))
    subtotal = float(price_per_kg or 0) * weight_kg
    return compute_gst(subtotal)


def technician_total(base_fee: float, hours: float = 1.0) -> PriceBreakdown:
    hours = max(1.0, float(hours or 1.0))
    subtotal = float(base_fee or 0) * hours
    return compute_gst(subtotal)


def flight_total(base_fare: float, passengers: int = 1) -> PriceBreakdown:
    passengers = max(1, int(passengers or 1))
    subtotal = float(base_fare or 0) * passengers
    return compute_gst(subtotal)

