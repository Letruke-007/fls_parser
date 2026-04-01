"""Convert FLS parse_statement result to unified parsed_document format."""

from __future__ import annotations
from decimal import Decimal
from typing import Any


def _split_name(full_name: str | None) -> dict[str, str | None]:
    if not full_name:
        return {"last_name": None, "first_name": None, "middle_name": None}
    parts = full_name.strip().split()
    return {
        "last_name": parts[0] if len(parts) > 0 else None,
        "first_name": parts[1] if len(parts) > 1 else None,
        "middle_name": parts[2] if len(parts) > 2 else None,
    }


def _serialize_decimal(v: Any) -> Any:
    """Convert Decimal to float for JSON serialization."""
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, dict):
        return {k: _serialize_decimal(val) for k, val in v.items()}
    if isinstance(v, list):
        return [_serialize_decimal(item) for item in v]
    return v


def normalize(result: dict[str, Any], source_filename: str = "input.rtf") -> dict[str, Any]:
    """Convert FLS result dict to unified parsed_document format."""
    address = result.get("address", {}) or {}

    persons = []
    if result.get("account_holder_name"):
        persons.append({
            "role": "owner",
            "full_name": result["account_holder_name"],
            **_split_name(result["account_holder_name"]),
            "birthday_date": None,
            "ownership_share": None,
            "identity": None,
            "departure": None,
        })

    return _serialize_decimal({
        "document_type": "fls",
        "source_filename": result.get("source_filename", source_filename),
        "address_raw": result.get("address_raw"),
        "address": {
            "raw": address.get("raw") or address.get("full"),
            "full": address.get("full"),
            "street": address.get("street"),
            "house": address.get("house"),
            "building": address.get("building"),
            "structure": address.get("structure"),
            "apartment": address.get("apartment"),
        },
        "persons": persons,
        "management_company": None,
        "property_type": None,
        "benefits": None,
        "billing": {
            "charges": result.get("charges"),
            "year_totals": result.get("year_totals"),
            "grand_total": result.get("grand_total"),
        },
        "validations": result.get("validations"),
        "metadata": {
            "parsing": result.get("parsing"),
        },
    })
