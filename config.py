# config.py
from __future__ import annotations
import json
import os
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Optional

from pydantic import BaseModel


_CONFIG_PATH = os.environ.get("WMS_CONFIG_PATH", "config.json")


class WMSSettings(BaseModel):
    base_url: str
    username: str
    password: str
    verify_ssl: bool = True
    default_timeout: float = 30.0
    default_retries: int = 3


class DatabaseSettings(BaseModel):
    host: str
    port: int
    user: str
    password: str
    database: str


class AppConfig(BaseModel):
    wms: WMSSettings
    database: DatabaseSettings

    @classmethod
    def load(cls, path: Optional[str] = None):
        p = path or _CONFIG_PATH
        if not os.path.exists(p):
            raise FileNotFoundError(f"Config file not found: {p}")
        with open(p, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return cls.parse_obj(raw)


# convenience functions used by other modules (keeps old names)
_cfg: Optional[AppConfig] = None


def load_config(path: Optional[str] = None) -> AppConfig:
    global _cfg
    if _cfg is None:
        _cfg = AppConfig.load(path)
    return _cfg


def get_wms_config() -> dict:
    cfg = load_config()
    return cfg.wms.dict()


def get_database_config() -> dict:
    cfg = load_config()
    return cfg.database.dict()


@dataclass
class DayRange:
    start: datetime
    end: datetime


def get_today_range(tz_naive: bool = True) -> DayRange:
    """
    Default: today midnight 00:00:00 -> next midnight (local machine time).
    If you need timezone-aware logic, modify to accept tzinfo.
    """
    now = datetime.now()
    start = datetime.combine(now.date(), time.min)
    end = start + timedelta(days=1)
    if tz_naive:
        return DayRange(start=start, end=end)
    return DayRange(start=start, end=end)
