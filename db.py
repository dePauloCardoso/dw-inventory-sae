from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

import psycopg
from psycopg.rows import dict_row
from pydantic import BaseModel

from config import get_database_config


class DatabaseSettings(BaseModel):
    pg_host: str
    pg_port: int
    pg_user: str
    pg_password: str
    pg_database: str
    
    def __init__(self, **data):
        # Load from config.json if not provided
        if not data:
            db_config = get_database_config()
            data = {
                "pg_host": db_config.get("host"),
                "pg_port": db_config.get("port"),
                "pg_user": db_config.get("user"),
                "pg_password": db_config.get("password"),
                "pg_database": db_config.get("database"),
            }
        
        super().__init__(**data)
        
        # Validate that all required fields are set
        required_vars = ["pg_host", "pg_port", "pg_user", "pg_password", "pg_database"]
        missing_vars = [var for var in required_vars if not getattr(self, var)]
        if missing_vars:
            raise ValueError(
                f"Missing required database configuration: {', '.join(missing_vars)}\n"
                "Please check your config.json file and ensure all database settings are present."
            )
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def get_connection(settings: Optional[DatabaseSettings] = None) -> psycopg.Connection:
    cfg = settings or DatabaseSettings()
    conn = psycopg.connect(
        host=cfg.pg_host,
        port=cfg.pg_port,
        user=cfg.pg_user,
        password=cfg.pg_password,
        dbname=cfg.pg_database,
        autocommit=False,
        row_factory=dict_row,
    )
    return conn




def upsert_inventory(conn: psycopg.Connection, rows: Iterable[Dict[str, Any]]) -> int:
    items: List[Dict[str, Any]] = list(rows)
    if not items:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.raw_inventory (
                id, url, create_user, create_ts, mod_user, mod_ts,
                facility_id_id, facility_id_key, facility_id_url,
                item_id_id, item_id_key, item_id_url,
                location_id, container_id_id, container_id_key, container_id_url,
                priority_date, curr_qty, orig_qty, pack_qty, case_qty, status_id,
                manufacture_date, expiry_date, batch_number_id,
                invn_attr_id_id, invn_attr_id_key, invn_attr_id_url,
                serial_nbr_set, uom_id_id, uom_id_key, uom_id_url
            ) values (
                %(id)s, %(url)s, %(create_user)s, %(create_ts)s, %(mod_user)s, %(mod_ts)s,
                %(facility_id_id)s, %(facility_id_key)s, %(facility_id_url)s,
                %(item_id_id)s, %(item_id_key)s, %(item_id_url)s,
                %(location_id)s, %(container_id_id)s, %(container_id_key)s, %(container_id_url)s,
                %(priority_date)s, %(curr_qty)s, %(orig_qty)s, %(pack_qty)s, %(case_qty)s, %(status_id)s,
                %(manufacture_date)s, %(expiry_date)s, %(batch_number_id)s,
                %(invn_attr_id_id)s, %(invn_attr_id_key)s, %(invn_attr_id_url)s,
                %(serial_nbr_set)s, %(uom_id_id)s, %(uom_id_key)s, %(uom_id_url)s
            ) on conflict (id) do update set
                url = excluded.url, mod_user = excluded.mod_user, mod_ts = excluded.mod_ts,
                facility_id_id = excluded.facility_id_id, facility_id_key = excluded.facility_id_key, facility_id_url = excluded.facility_id_url,
                item_id_id = excluded.item_id_id, item_id_key = excluded.item_id_key, item_id_url = excluded.item_id_url,
                location_id = excluded.location_id, container_id_id = excluded.container_id_id, container_id_key = excluded.container_id_key, container_id_url = excluded.container_id_url,
                priority_date = excluded.priority_date, curr_qty = excluded.curr_qty, orig_qty = excluded.orig_qty, pack_qty = excluded.pack_qty, case_qty = excluded.case_qty, status_id = excluded.status_id,
                manufacture_date = excluded.manufacture_date, expiry_date = excluded.expiry_date, batch_number_id = excluded.batch_number_id,
                invn_attr_id_id = excluded.invn_attr_id_id, invn_attr_id_key = excluded.invn_attr_id_key, invn_attr_id_url = excluded.invn_attr_id_url,
                serial_nbr_set = excluded.serial_nbr_set, uom_id_id = excluded.uom_id_id, uom_id_key = excluded.uom_id_key, uom_id_url = excluded.uom_id_url
            """,
            [
                {
                    "id": item.get("id"),
                    "url": item.get("url"),
                    "create_user": item.get("create_user"),
                    "create_ts": item.get("create_ts"),
                    "mod_user": item.get("mod_user"),
                    "mod_ts": item.get("mod_ts"),
                    "facility_id_id": item.get("facility_id.id"),
                    "facility_id_key": item.get("facility_id.key"),
                    "facility_id_url": item.get("facility_id.url"),
                    "item_id_id": item.get("item_id.id"),
                    "item_id_key": item.get("item_id.key"),
                    "item_id_url": item.get("item_id.url"),
                    "location_id": item.get("location_id"),
                    "container_id_id": item.get("container_id.id"),
                    "container_id_key": item.get("container_id.key"),
                    "container_id_url": item.get("container_id.url"),
                    "priority_date": item.get("priority_date"),
                    "curr_qty": item.get("curr_qty"),
                    "orig_qty": item.get("orig_qty"),
                    "pack_qty": item.get("pack_qty"),
                    "case_qty": item.get("case_qty"),
                    "status_id": item.get("status_id"),
                    "manufacture_date": item.get("manufacture_date"),
                    "expiry_date": item.get("expiry_date"),
                    "batch_number_id": item.get("batch_number_id"),
                    "invn_attr_id_id": item.get("invn_attr_id.id"),
                    "invn_attr_id_key": item.get("invn_attr_id.key"),
                    "invn_attr_id_url": item.get("invn_attr_id.url"),
                    "serial_nbr_set": item.get("serial_nbr_set"),
                    "uom_id_id": item.get("uom_id.id"),
                    "uom_id_key": item.get("uom_id.key"),
                    "uom_id_url": item.get("uom_id.url"),
                }
                for item in items
            ],
        )
    conn.commit()
    return len(items)


def upsert_container(conn: psycopg.Connection, rows: Iterable[Dict[str, Any]]) -> int:
    items: List[Dict[str, Any]] = list(rows)
    if not items:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.raw_container (
                id, url, create_user, create_ts, mod_user, mod_ts,
                facility_id_id, facility_id_key, facility_id_url,
                company_id_id, company_id_key, company_id_url,
                container_nbr, type, status_id, vas_status_id,
                curr_location_id, prev_location_id, priority_date, pallet_id,
                rcvd_shipment_id_id, rcvd_shipment_id_key, rcvd_shipment_id_url,
                rcvd_ts, rcvd_user, weight, volume, pick_user, pack_user,
                putawaytype_id_id, putawaytype_id_key, putawaytype_id_url,
                ref_iblpn_nbr, ref_shipment_nbr, ref_po_nbr, ref_oblpn_nbr,
                first_putaway_ts, parcel_batch_flg, lpn_type_id, cart_posn_nbr,
                audit_status_id, qc_status_id, asset_id, asset_seal_nbr,
                price_labels_printed, comments, actual_weight_flg,
                length, width, height, rcvd_trailer_nbr, orig_container_nbr,
                inventory_lock_set, nbr_files, cust_field_1, cust_field_2,
                cust_field_3, cust_field_4, cust_field_5, cart_nbr
            ) values (
                %(id)s, %(url)s, %(create_user)s, %(create_ts)s, %(mod_user)s, %(mod_ts)s,
                %(facility_id_id)s, %(facility_id_key)s, %(facility_id_url)s,
                %(company_id_id)s, %(company_id_key)s, %(company_id_url)s,
                %(container_nbr)s, %(type)s, %(status_id)s, %(vas_status_id)s,
                %(curr_location_id)s, %(prev_location_id)s, %(priority_date)s, %(pallet_id)s,
                %(rcvd_shipment_id_id)s, %(rcvd_shipment_id_key)s, %(rcvd_shipment_id_url)s,
                %(rcvd_ts)s, %(rcvd_user)s, %(weight)s, %(volume)s, %(pick_user)s, %(pack_user)s,
                %(putawaytype_id_id)s, %(putawaytype_id_key)s, %(putawaytype_id_url)s,
                %(ref_iblpn_nbr)s, %(ref_shipment_nbr)s, %(ref_po_nbr)s, %(ref_oblpn_nbr)s,
                %(first_putaway_ts)s, %(parcel_batch_flg)s, %(lpn_type_id)s, %(cart_posn_nbr)s,
                %(audit_status_id)s, %(qc_status_id)s, %(asset_id)s, %(asset_seal_nbr)s,
                %(price_labels_printed)s, %(comments)s, %(actual_weight_flg)s,
                %(length)s, %(width)s, %(height)s, %(rcvd_trailer_nbr)s, %(orig_container_nbr)s,
                %(inventory_lock_set)s, %(nbr_files)s, %(cust_field_1)s, %(cust_field_2)s,
                %(cust_field_3)s, %(cust_field_4)s, %(cust_field_5)s, %(cart_nbr)s
            ) on conflict (id) do update set
                url = excluded.url, mod_user = excluded.mod_user, mod_ts = excluded.mod_ts,
                facility_id_id = excluded.facility_id_id, facility_id_key = excluded.facility_id_key, facility_id_url = excluded.facility_id_url,
                company_id_id = excluded.company_id_id, company_id_key = excluded.company_id_key, company_id_url = excluded.company_id_url,
                container_nbr = excluded.container_nbr, type = excluded.type, status_id = excluded.status_id, vas_status_id = excluded.vas_status_id,
                curr_location_id = excluded.curr_location_id, prev_location_id = excluded.prev_location_id, priority_date = excluded.priority_date, pallet_id = excluded.pallet_id,
                rcvd_shipment_id_id = excluded.rcvd_shipment_id_id, rcvd_shipment_id_key = excluded.rcvd_shipment_id_key, rcvd_shipment_id_url = excluded.rcvd_shipment_id_url,
                rcvd_ts = excluded.rcvd_ts, rcvd_user = excluded.rcvd_user, weight = excluded.weight, volume = excluded.volume, pick_user = excluded.pick_user, pack_user = excluded.pack_user,
                putawaytype_id_id = excluded.putawaytype_id_id, putawaytype_id_key = excluded.putawaytype_id_key, putawaytype_id_url = excluded.putawaytype_id_url,
                ref_iblpn_nbr = excluded.ref_iblpn_nbr, ref_shipment_nbr = excluded.ref_shipment_nbr, ref_po_nbr = excluded.ref_po_nbr, ref_oblpn_nbr = excluded.ref_oblpn_nbr,
                first_putaway_ts = excluded.first_putaway_ts, parcel_batch_flg = excluded.parcel_batch_flg, lpn_type_id = excluded.lpn_type_id, cart_posn_nbr = excluded.cart_posn_nbr,
                audit_status_id = excluded.audit_status_id, qc_status_id = excluded.qc_status_id, asset_id = excluded.asset_id, asset_seal_nbr = excluded.asset_seal_nbr,
                price_labels_printed = excluded.price_labels_printed, comments = excluded.comments, actual_weight_flg = excluded.actual_weight_flg,
                length = excluded.length, width = excluded.width, height = excluded.height, rcvd_trailer_nbr = excluded.rcvd_trailer_nbr, orig_container_nbr = excluded.orig_container_nbr,
                inventory_lock_set = excluded.inventory_lock_set, nbr_files = excluded.nbr_files, cust_field_1 = excluded.cust_field_1, cust_field_2 = excluded.cust_field_2,
                cust_field_3 = excluded.cust_field_3, cust_field_4 = excluded.cust_field_4, cust_field_5 = excluded.cust_field_5, cart_nbr = excluded.cart_nbr
            """,
            [
                {
                    "id": item.get("id"),
                    "url": item.get("url"),
                    "create_user": item.get("create_user"),
                    "create_ts": item.get("create_ts"),
                    "mod_user": item.get("mod_user"),
                    "mod_ts": item.get("mod_ts"),
                    "facility_id_id": item.get("facility_id.id"),
                    "facility_id_key": item.get("facility_id.key"),
                    "facility_id_url": item.get("facility_id.url"),
                    "company_id_id": item.get("company_id.id"),
                    "company_id_key": item.get("company_id.key"),
                    "company_id_url": item.get("company_id.url"),
                    "container_nbr": item.get("container_nbr"),
                    "type": item.get("type"),
                    "status_id": item.get("status_id"),
                    "vas_status_id": item.get("vas_status_id"),
                    "curr_location_id": item.get("curr_location_id"),
                    "prev_location_id": item.get("prev_location_id"),
                    "priority_date": item.get("priority_date"),
                    "pallet_id": item.get("pallet_id"),
                    "rcvd_shipment_id_id": item.get("rcvd_shipment_id.id"),
                    "rcvd_shipment_id_key": item.get("rcvd_shipment_id.key"),
                    "rcvd_shipment_id_url": item.get("rcvd_shipment_id.url"),
                    "rcvd_ts": item.get("rcvd_ts"),
                    "rcvd_user": item.get("rcvd_user"),
                    "weight": item.get("weight"),
                    "volume": item.get("volume"),
                    "pick_user": item.get("pick_user"),
                    "pack_user": item.get("pack_user"),
                    "putawaytype_id_id": item.get("putawaytype_id.id"),
                    "putawaytype_id_key": item.get("putawaytype_id.key"),
                    "putawaytype_id_url": item.get("putawaytype_id.url"),
                    "ref_iblpn_nbr": item.get("ref_iblpn_nbr"),
                    "ref_shipment_nbr": item.get("ref_shipment_nbr"),
                    "ref_po_nbr": item.get("ref_po_nbr"),
                    "ref_oblpn_nbr": item.get("ref_oblpn_nbr"),
                    "first_putaway_ts": item.get("first_putaway_ts"),
                    "parcel_batch_flg": item.get("parcel_batch_flg"),
                    "lpn_type_id": item.get("lpn_type_id"),
                    "cart_posn_nbr": item.get("cart_posn_nbr"),
                    "audit_status_id": item.get("audit_status_id"),
                    "qc_status_id": item.get("qc_status_id"),
                    "asset_id": item.get("asset_id"),
                    "asset_seal_nbr": item.get("asset_seal_nbr"),
                    "price_labels_printed": item.get("price_labels_printed"),
                    "comments": item.get("comments"),
                    "actual_weight_flg": item.get("actual_weight_flg"),
                    "length": item.get("length"),
                    "width": item.get("width"),
                    "height": item.get("height"),
                    "rcvd_trailer_nbr": item.get("rcvd_trailer_nbr"),
                    "orig_container_nbr": item.get("orig_container_nbr"),
                    "inventory_lock_set": item.get("inventory_lock_set"),
                    "nbr_files": item.get("nbr_files"),
                    "cust_field_1": item.get("cust_field_1"),
                    "cust_field_2": item.get("cust_field_2"),
                    "cust_field_3": item.get("cust_field_3"),
                    "cust_field_4": item.get("cust_field_4"),
                    "cust_field_5": item.get("cust_field_5"),
                    "cart_nbr": item.get("cart_nbr"),
                }
                for item in items
            ],
        )
    conn.commit()
    return len(items)


def upsert_container_status(conn: psycopg.Connection, rows: Iterable[Dict[str, Any]]) -> int:
    items: List[Dict[str, Any]] = list(rows)
    if not items:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.raw_container_status (
                id, description
            ) values (
                %(id)s, %(description)s
            ) on conflict (id) do update set
                description = excluded.description
            """,
            [
                {
                    "id": item.get("id"),
                    "description": item.get("description"),
                }
                for item in items
            ],
        )
    conn.commit()
    return len(items)


