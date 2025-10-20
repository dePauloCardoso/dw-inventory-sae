from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Dict, Any


@dataclass
class DateRange:
    start: datetime
    end: datetime


def load_config() -> Dict[str, Any]:
    """Load configuration from config.json file"""
    config_path = os.path.join(os.path.dirname(__file__), "config.json")

    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            "Please create a config.json file with the required settings."
        )

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    return config


def get_today_range(tz_offset_hours: int = 0) -> DateRange:
    tz = timezone(timedelta(hours=tz_offset_hours))
    now = datetime.now(tz)
    start_dt = datetime.combine(now.date(), time(0, 0, 0), tzinfo=tz)
    end_dt = datetime.combine(now.date(), time(23, 59, 59), tzinfo=tz)
    return DateRange(start=start_dt, end=end_dt)


def get_wms_config() -> Dict[str, Any]:
    """Get WMS configuration from config.json"""
    config = load_config()
    wms_config = config.get("wms", {})

    if not wms_config:
        raise RuntimeError("WMS configuration not found in config.json")

    return wms_config


def get_database_config() -> Dict[str, Any]:
    """Get database configuration from config.json"""
    config = load_config()
    db_config = config.get("database", {})

    if not db_config:
        raise RuntimeError("Database configuration not found in config.json")

    return db_config
