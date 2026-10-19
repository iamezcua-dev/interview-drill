"""Map neutral column types (tables.yaml) to each engine."""

import datetime as dt
import re
from decimal import Decimal
from typing import Any

_PARAMS = re.compile(r"^(\w+)(?:\((.*)\))?$")


def _split(neutral: str) -> tuple[str, list[str]]:
    match = _PARAMS.match(neutral)
    base, args = match.group(1), match.group(2)
    return base, [a.strip() for a in args.split(",")] if args else []


def tsql(neutral: str) -> str:
    base, args = _split(neutral)
    if base == "string":
        return f"VARCHAR({args[0] if args else 'MAX'})"
    if base == "decimal":
        return f"DECIMAL({args[0]},{args[1]})"
    return {
        "int": "INT",
        "bigint": "BIGINT",
        "double": "FLOAT",
        "date": "DATE",
        "timestamp": "DATETIME2",
        "boolean": "BIT",
    }[base]


def spark(neutral: str) -> str:
    """Spark SQL DDL type (also understood by Scala's StructType.fromDDL)."""
    base, args = _split(neutral)
    if base == "string":
        return "STRING"
    if base == "decimal":
        return f"DECIMAL({args[0]},{args[1]})"
    return {
        "int": "INT",
        "bigint": "BIGINT",
        "double": "DOUBLE",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "boolean": "BOOLEAN",
    }[base]


def coerce(neutral: str, value: Any) -> Any:
    """Turn a YAML value into the Python type each driver expects."""
    if value is None:
        return None
    base, _ = _split(neutral)
    if base in ("int", "bigint"):
        return int(value)
    if base == "double":
        return float(value)
    if base == "decimal":
        return Decimal(str(value))
    if base == "date":
        if isinstance(value, dt.datetime):
            return value.date()
        if isinstance(value, dt.date):
            return value
        return dt.date.fromisoformat(str(value))
    if base == "timestamp":
        if isinstance(value, dt.datetime):
            return value
        return dt.datetime.fromisoformat(str(value))
    if base == "boolean":
        return bool(value)
    return str(value)


def to_json(value: Any) -> Any:
    """JSON-safe form used to ship rows to the Scala harness."""
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value
