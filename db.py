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


def upsert_container_status(
    conn: psycopg.Connection, rows: Iterable[Dict[str, Any]]
) -> int:
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


def upsert_order_hdr(conn: psycopg.Connection, rows: Iterable[Dict[str, Any]]) -> int:
    items: List[Dict[str, Any]] = list(rows)
    if not items:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.raw_order_hdr (
                id, url, create_user, create_ts, mod_user, mod_ts,
                facility_id_id, facility_id_key, facility_id_url,
                company_id_id, company_id_key, company_id_url,
                order_nbr, order_type_id_id, order_type_id_key, order_type_id_url,
                status_id, ord_date, exp_date, req_ship_date, dest_facility_id, shipto_facility_id,
                cust_name, cust_addr, cust_addr2, cust_addr3, cust_city, cust_state, cust_zip, cust_country,
                cust_phone_nbr, cust_email, cust_nbr, shipto_name, shipto_addr, shipto_addr2, shipto_addr3,
                shipto_city, shipto_state, shipto_zip, shipto_country, shipto_phone_nbr, shipto_email,
                ref_nbr, stage_location_id, ship_via_ref_code, route_nbr, external_route,
                destination_company_id_id, destination_company_id_key, destination_company_id_url,
                ship_via_id, priority, host_allocation_nbr, sales_order_nbr, sales_channel, customer_po_nbr,
                carrier_account_nbr, payment_method_id, dest_dept_nbr, start_ship_date, stop_ship_date,
                vas_group_code, spl_instr, currency_code, record_origin_code, cust_contact, shipto_contact,
                ob_lpn_type, ob_lpn_type_id, total_orig_ord_qty, orig_sku_count, orig_sale_price, gift_msg,
                sched_ship_date, customer_po_type, customer_vendor_code, externally_planned_load_flg,
                work_order_kit_id, order_nbr_to_replace, stop_ship_flg, lpn_type_class,
                billto_carrier_account_nbr, duties_carrier_account_nbr, duties_payment_method_id,
                customs_broker_contact_id, order_shipped_ts, cust_field_1, cust_field_2, cust_field_3,
                cust_field_4, cust_field_5, cust_date_1, cust_date_2, cust_date_3, cust_date_4, cust_date_5,
                cust_number_1, cust_number_2, cust_number_3, cust_number_4, cust_number_5,
                cust_decimal_1, cust_decimal_2, cust_decimal_3, cust_decimal_4, cust_decimal_5,
                cust_short_text_1, cust_short_text_2, cust_short_text_3, cust_short_text_4, cust_short_text_5,
                cust_short_text_6, cust_short_text_7, cust_short_text_8, cust_short_text_9, cust_short_text_10,
                cust_short_text_11, cust_short_text_12, cust_long_text_1, cust_long_text_2, cust_long_text_3,
                order_instructions_set, order_dtl_set_result_count, order_dtl_set_url, order_lock_set,
                tms_parcel_shipment_nbr, erp_source_hdr_ref, erp_source_system_ref, tms_order_hdr_ref, group_ref
            ) values (
                %(id)s, %(url)s, %(create_user)s, %(create_ts)s, %(mod_user)s, %(mod_ts)s,
                %(facility_id_id)s, %(facility_id_key)s, %(facility_id_url)s,
                %(company_id_id)s, %(company_id_key)s, %(company_id_url)s,
                %(order_nbr)s, %(order_type_id_id)s, %(order_type_id_key)s, %(order_type_id_url)s,
                %(status_id)s, %(ord_date)s, %(exp_date)s, %(req_ship_date)s, %(dest_facility_id)s, %(shipto_facility_id)s,
                %(cust_name)s, %(cust_addr)s, %(cust_addr2)s, %(cust_addr3)s, %(cust_city)s, %(cust_state)s, %(cust_zip)s, %(cust_country)s,
                %(cust_phone_nbr)s, %(cust_email)s, %(cust_nbr)s, %(shipto_name)s, %(shipto_addr)s, %(shipto_addr2)s, %(shipto_addr3)s,
                %(shipto_city)s, %(shipto_state)s, %(shipto_zip)s, %(shipto_country)s, %(shipto_phone_nbr)s, %(shipto_email)s,
                %(ref_nbr)s, %(stage_location_id)s, %(ship_via_ref_code)s, %(route_nbr)s, %(external_route)s,
                %(destination_company_id_id)s, %(destination_company_id_key)s, %(destination_company_id_url)s,
                %(ship_via_id)s, %(priority)s, %(host_allocation_nbr)s, %(sales_order_nbr)s, %(sales_channel)s, %(customer_po_nbr)s,
                %(carrier_account_nbr)s, %(payment_method_id)s, %(dest_dept_nbr)s, %(start_ship_date)s, %(stop_ship_date)s,
                %(vas_group_code)s, %(spl_instr)s, %(currency_code)s, %(record_origin_code)s, %(cust_contact)s, %(shipto_contact)s,
                %(ob_lpn_type)s, %(ob_lpn_type_id)s, %(total_orig_ord_qty)s, %(orig_sku_count)s, %(orig_sale_price)s, %(gift_msg)s,
                %(sched_ship_date)s, %(customer_po_type)s, %(customer_vendor_code)s, %(externally_planned_load_flg)s,
                %(work_order_kit_id)s, %(order_nbr_to_replace)s, %(stop_ship_flg)s, %(lpn_type_class)s,
                %(billto_carrier_account_nbr)s, %(duties_carrier_account_nbr)s, %(duties_payment_method_id)s,
                %(customs_broker_contact_id)s, %(order_shipped_ts)s, %(cust_field_1)s, %(cust_field_2)s, %(cust_field_3)s,
                %(cust_field_4)s, %(cust_field_5)s, %(cust_date_1)s, %(cust_date_2)s, %(cust_date_3)s, %(cust_date_4)s, %(cust_date_5)s,
                %(cust_number_1)s, %(cust_number_2)s, %(cust_number_3)s, %(cust_number_4)s, %(cust_number_5)s,
                %(cust_decimal_1)s, %(cust_decimal_2)s, %(cust_decimal_3)s, %(cust_decimal_4)s, %(cust_decimal_5)s,
                %(cust_short_text_1)s, %(cust_short_text_2)s, %(cust_short_text_3)s, %(cust_short_text_4)s, %(cust_short_text_5)s,
                %(cust_short_text_6)s, %(cust_short_text_7)s, %(cust_short_text_8)s, %(cust_short_text_9)s, %(cust_short_text_10)s,
                %(cust_short_text_11)s, %(cust_short_text_12)s, %(cust_long_text_1)s, %(cust_long_text_2)s, %(cust_long_text_3)s,
                %(order_instructions_set)s, %(order_dtl_set_result_count)s, %(order_dtl_set_url)s, %(order_lock_set)s,
                %(tms_parcel_shipment_nbr)s, %(erp_source_hdr_ref)s, %(erp_source_system_ref)s, %(tms_order_hdr_ref)s, %(group_ref)s
            ) on conflict (id) do update set
                url = excluded.url, mod_user = excluded.mod_user, mod_ts = excluded.mod_ts,
                facility_id_id = excluded.facility_id_id, facility_id_key = excluded.facility_id_key, facility_id_url = excluded.facility_id_url,
                company_id_id = excluded.company_id_id, company_id_key = excluded.company_id_key, company_id_url = excluded.company_id_url,
                order_nbr = excluded.order_nbr, order_type_id_id = excluded.order_type_id_id, order_type_id_key = excluded.order_type_id_key, order_type_id_url = excluded.order_type_id_url,
                status_id = excluded.status_id, ord_date = excluded.ord_date, exp_date = excluded.exp_date, req_ship_date = excluded.req_ship_date, dest_facility_id = excluded.dest_facility_id, shipto_facility_id = excluded.shipto_facility_id,
                cust_name = excluded.cust_name, cust_addr = excluded.cust_addr, cust_addr2 = excluded.cust_addr2, cust_addr3 = excluded.cust_addr3, cust_city = excluded.cust_city, cust_state = excluded.cust_state, cust_zip = excluded.cust_zip, cust_country = excluded.cust_country,
                cust_phone_nbr = excluded.cust_phone_nbr, cust_email = excluded.cust_email, cust_nbr = excluded.cust_nbr, shipto_name = excluded.shipto_name, shipto_addr = excluded.shipto_addr, shipto_addr2 = excluded.shipto_addr2, shipto_addr3 = excluded.shipto_addr3,
                shipto_city = excluded.shipto_city, shipto_state = excluded.shipto_state, shipto_zip = excluded.shipto_zip, shipto_country = excluded.shipto_country, shipto_phone_nbr = excluded.shipto_phone_nbr, shipto_email = excluded.shipto_email,
                ref_nbr = excluded.ref_nbr, stage_location_id = excluded.stage_location_id, ship_via_ref_code = excluded.ship_via_ref_code, route_nbr = excluded.route_nbr, external_route = excluded.external_route,
                destination_company_id_id = excluded.destination_company_id_id, destination_company_id_key = excluded.destination_company_id_key, destination_company_id_url = excluded.destination_company_id_url,
                ship_via_id = excluded.ship_via_id, priority = excluded.priority, host_allocation_nbr = excluded.host_allocation_nbr, sales_order_nbr = excluded.sales_order_nbr, sales_channel = excluded.sales_channel, customer_po_nbr = excluded.customer_po_nbr,
                carrier_account_nbr = excluded.carrier_account_nbr, payment_method_id = excluded.payment_method_id, dest_dept_nbr = excluded.dest_dept_nbr, start_ship_date = excluded.start_ship_date, stop_ship_date = excluded.stop_ship_date,
                vas_group_code = excluded.vas_group_code, spl_instr = excluded.spl_instr, currency_code = excluded.currency_code, record_origin_code = excluded.record_origin_code, cust_contact = excluded.cust_contact, shipto_contact = excluded.shipto_contact,
                ob_lpn_type = excluded.ob_lpn_type, ob_lpn_type_id = excluded.ob_lpn_type_id, total_orig_ord_qty = excluded.total_orig_ord_qty, orig_sku_count = excluded.orig_sku_count, orig_sale_price = excluded.orig_sale_price, gift_msg = excluded.gift_msg,
                sched_ship_date = excluded.sched_ship_date, customer_po_type = excluded.customer_po_type, customer_vendor_code = excluded.customer_vendor_code, externally_planned_load_flg = excluded.externally_planned_load_flg,
                work_order_kit_id = excluded.work_order_kit_id, order_nbr_to_replace = excluded.order_nbr_to_replace, stop_ship_flg = excluded.stop_ship_flg, lpn_type_class = excluded.lpn_type_class,
                billto_carrier_account_nbr = excluded.billto_carrier_account_nbr, duties_carrier_account_nbr = excluded.duties_carrier_account_nbr, duties_payment_method_id = excluded.duties_payment_method_id,
                customs_broker_contact_id = excluded.customs_broker_contact_id, order_shipped_ts = excluded.order_shipped_ts, cust_field_1 = excluded.cust_field_1, cust_field_2 = excluded.cust_field_2, cust_field_3 = excluded.cust_field_3,
                cust_field_4 = excluded.cust_field_4, cust_field_5 = excluded.cust_field_5, cust_date_1 = excluded.cust_date_1, cust_date_2 = excluded.cust_date_2, cust_date_3 = excluded.cust_date_3, cust_date_4 = excluded.cust_date_4, cust_date_5 = excluded.cust_date_5,
                cust_number_1 = excluded.cust_number_1, cust_number_2 = excluded.cust_number_2, cust_number_3 = excluded.cust_number_3, cust_number_4 = excluded.cust_number_4, cust_number_5 = excluded.cust_number_5,
                cust_decimal_1 = excluded.cust_decimal_1, cust_decimal_2 = excluded.cust_decimal_2, cust_decimal_3 = excluded.cust_decimal_3, cust_decimal_4 = excluded.cust_decimal_4, cust_decimal_5 = excluded.cust_decimal_5,
                cust_short_text_1 = excluded.cust_short_text_1, cust_short_text_2 = excluded.cust_short_text_2, cust_short_text_3 = excluded.cust_short_text_3, cust_short_text_4 = excluded.cust_short_text_4, cust_short_text_5 = excluded.cust_short_text_5,
                cust_short_text_6 = excluded.cust_short_text_6, cust_short_text_7 = excluded.cust_short_text_7, cust_short_text_8 = excluded.cust_short_text_8, cust_short_text_9 = excluded.cust_short_text_9, cust_short_text_10 = excluded.cust_short_text_10,
                cust_short_text_11 = excluded.cust_short_text_11, cust_short_text_12 = excluded.cust_short_text_12, cust_long_text_1 = excluded.cust_long_text_1, cust_long_text_2 = excluded.cust_long_text_2, cust_long_text_3 = excluded.cust_long_text_3,
                order_instructions_set = excluded.order_instructions_set, order_dtl_set_result_count = excluded.order_dtl_set_result_count, order_dtl_set_url = excluded.order_dtl_set_url, order_lock_set = excluded.order_lock_set,
                tms_parcel_shipment_nbr = excluded.tms_parcel_shipment_nbr, erp_source_hdr_ref = excluded.erp_source_hdr_ref, erp_source_system_ref = excluded.erp_source_system_ref, tms_order_hdr_ref = excluded.tms_order_hdr_ref, group_ref = excluded.group_ref
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
                    "order_nbr": item.get("order_nbr"),
                    "order_type_id_id": item.get("order_type_id.id"),
                    "order_type_id_key": item.get("order_type_id.key"),
                    "order_type_id_url": item.get("order_type_id.url"),
                    "status_id": item.get("status_id"),
                    "ord_date": item.get("ord_date"),
                    "exp_date": item.get("exp_date"),
                    "req_ship_date": item.get("req_ship_date"),
                    "dest_facility_id": item.get("dest_facility_id"),
                    "shipto_facility_id": item.get("shipto_facility_id"),
                    "cust_name": item.get("cust_name"),
                    "cust_addr": item.get("cust_addr"),
                    "cust_addr2": item.get("cust_addr2"),
                    "cust_addr3": item.get("cust_addr3"),
                    "cust_city": item.get("cust_city"),
                    "cust_state": item.get("cust_state"),
                    "cust_zip": item.get("cust_zip"),
                    "cust_country": item.get("cust_country"),
                    "cust_phone_nbr": item.get("cust_phone_nbr"),
                    "cust_email": item.get("cust_email"),
                    "cust_nbr": item.get("cust_nbr"),
                    "shipto_name": item.get("shipto_name"),
                    "shipto_addr": item.get("shipto_addr"),
                    "shipto_addr2": item.get("shipto_addr2"),
                    "shipto_addr3": item.get("shipto_addr3"),
                    "shipto_city": item.get("shipto_city"),
                    "shipto_state": item.get("shipto_state"),
                    "shipto_zip": item.get("shipto_zip"),
                    "shipto_country": item.get("shipto_country"),
                    "shipto_phone_nbr": item.get("shipto_phone_nbr"),
                    "shipto_email": item.get("shipto_email"),
                    "ref_nbr": item.get("ref_nbr"),
                    "stage_location_id": item.get("stage_location_id"),
                    "ship_via_ref_code": item.get("ship_via_ref_code"),
                    "route_nbr": item.get("route_nbr"),
                    "external_route": item.get("external_route"),
                    "destination_company_id_id": item.get("destination_company_id.id"),
                    "destination_company_id_key": item.get(
                        "destination_company_id.key"
                    ),
                    "destination_company_id_url": item.get(
                        "destination_company_id.url"
                    ),
                    "ship_via_id": item.get("ship_via_id"),
                    "priority": item.get("priority"),
                    "host_allocation_nbr": item.get("host_allocation_nbr"),
                    "sales_order_nbr": item.get("sales_order_nbr"),
                    "sales_channel": item.get("sales_channel"),
                    "customer_po_nbr": item.get("customer_po_nbr"),
                    "carrier_account_nbr": item.get("carrier_account_nbr"),
                    "payment_method_id": item.get("payment_method_id"),
                    "dest_dept_nbr": item.get("dest_dept_nbr"),
                    "start_ship_date": item.get("start_ship_date"),
                    "stop_ship_date": item.get("stop_ship_date"),
                    "vas_group_code": item.get("vas_group_code"),
                    "spl_instr": item.get("spl_instr"),
                    "currency_code": item.get("currency_code"),
                    "record_origin_code": item.get("record_origin_code"),
                    "cust_contact": item.get("cust_contact"),
                    "shipto_contact": item.get("shipto_contact"),
                    "ob_lpn_type": item.get("ob_lpn_type"),
                    "ob_lpn_type_id": item.get("ob_lpn_type_id"),
                    "total_orig_ord_qty": item.get("total_orig_ord_qty"),
                    "orig_sku_count": item.get("orig_sku_count"),
                    "orig_sale_price": item.get("orig_sale_price"),
                    "gift_msg": item.get("gift_msg"),
                    "sched_ship_date": item.get("sched_ship_date"),
                    "customer_po_type": item.get("customer_po_type"),
                    "customer_vendor_code": item.get("customer_vendor_code"),
                    "externally_planned_load_flg": item.get(
                        "externally_planned_load_flg"
                    ),
                    "work_order_kit_id": item.get("work_order_kit_id"),
                    "order_nbr_to_replace": item.get("order_nbr_to_replace"),
                    "stop_ship_flg": item.get("stop_ship_flg"),
                    "lpn_type_class": item.get("lpn_type_class"),
                    "billto_carrier_account_nbr": item.get(
                        "billto_carrier_account_nbr"
                    ),
                    "duties_carrier_account_nbr": item.get(
                        "duties_carrier_account_nbr"
                    ),
                    "duties_payment_method_id": item.get("duties_payment_method_id"),
                    "customs_broker_contact_id": item.get("customs_broker_contact_id"),
                    "order_shipped_ts": item.get("order_shipped_ts"),
                    "cust_field_1": item.get("cust_field_1"),
                    "cust_field_2": item.get("cust_field_2"),
                    "cust_field_3": item.get("cust_field_3"),
                    "cust_field_4": item.get("cust_field_4"),
                    "cust_field_5": item.get("cust_field_5"),
                    "cust_date_1": item.get("cust_date_1"),
                    "cust_date_2": item.get("cust_date_2"),
                    "cust_date_3": item.get("cust_date_3"),
                    "cust_date_4": item.get("cust_date_4"),
                    "cust_date_5": item.get("cust_date_5"),
                    "cust_number_1": item.get("cust_number_1"),
                    "cust_number_2": item.get("cust_number_2"),
                    "cust_number_3": item.get("cust_number_3"),
                    "cust_number_4": item.get("cust_number_4"),
                    "cust_number_5": item.get("cust_number_5"),
                    "cust_decimal_1": item.get("cust_decimal_1"),
                    "cust_decimal_2": item.get("cust_decimal_2"),
                    "cust_decimal_3": item.get("cust_decimal_3"),
                    "cust_decimal_4": item.get("cust_decimal_4"),
                    "cust_decimal_5": item.get("cust_decimal_5"),
                    "cust_short_text_1": item.get("cust_short_text_1"),
                    "cust_short_text_2": item.get("cust_short_text_2"),
                    "cust_short_text_3": item.get("cust_short_text_3"),
                    "cust_short_text_4": item.get("cust_short_text_4"),
                    "cust_short_text_5": item.get("cust_short_text_5"),
                    "cust_short_text_6": item.get("cust_short_text_6"),
                    "cust_short_text_7": item.get("cust_short_text_7"),
                    "cust_short_text_8": item.get("cust_short_text_8"),
                    "cust_short_text_9": item.get("cust_short_text_9"),
                    "cust_short_text_10": item.get("cust_short_text_10"),
                    "cust_short_text_11": item.get("cust_short_text_11"),
                    "cust_short_text_12": item.get("cust_short_text_12"),
                    "cust_long_text_1": item.get("cust_long_text_1"),
                    "cust_long_text_2": item.get("cust_long_text_2"),
                    "cust_long_text_3": item.get("cust_long_text_3"),
                    "order_instructions_set": item.get("order_instructions_set"),
                    "order_dtl_set_result_count": item.get(
                        "order_dtl_set_result_count"
                    ),
                    "order_dtl_set_url": item.get("order_dtl_set_url"),
                    "order_lock_set": item.get("order_lock_set"),
                    "tms_parcel_shipment_nbr": item.get("tms_parcel_shipment_nbr"),
                    "erp_source_hdr_ref": item.get("erp_source_hdr_ref"),
                    "erp_source_system_ref": item.get("erp_source_system_ref"),
                    "tms_order_hdr_ref": item.get("tms_order_hdr_ref"),
                    "group_ref": item.get("group_ref"),
                }
                for item in items
            ],
        )
    conn.commit()
    return len(items)


def upsert_order_dtl(conn: psycopg.Connection, rows: Iterable[Dict[str, Any]]) -> int:
    items: List[Dict[str, Any]] = list(rows)
    if not items:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.raw_order_dtl (
                id, url, create_user, create_ts, mod_user, mod_ts,
                order_id_id, order_id_key, order_id_url, seq_nbr,
                item_id_id, item_id_key, item_id_url, ord_qty, orig_ord_qty, alloc_qty,
                req_cntr_nbr, po_nbr, shipment_nbr, dest_facility_attr_a, dest_facility_attr_b, dest_facility_attr_c,
                ref_nbr_1, vas_activity_code, cost, sale_price, host_ob_lpn_nbr, spl_instr,
                batch_number_id, voucher_nbr, voucher_amount, voucher_exp_date, req_pallet_nbr,
                lock_code, serial_nbr, voucher_print_count, ship_request_line, unit_declared_value,
                externally_planned_load_nbr, invn_attr_id_id, invn_attr_id_key, invn_attr_id_url,
                internal_text_field_1, orig_item_code, cust_field_1, cust_field_2, cust_field_3,
                cust_field_4, cust_field_5, cust_date_1, cust_date_2, cust_date_3, cust_date_4, cust_date_5,
                cust_number_1, cust_number_2, cust_number_3, cust_number_4, cust_number_5,
                cust_decimal_1, cust_decimal_2, cust_decimal_3, cust_decimal_4, cust_decimal_5,
                cust_short_text_1, cust_short_text_2, cust_short_text_3, cust_short_text_4, cust_short_text_5,
                cust_short_text_6, cust_short_text_7, cust_short_text_8, cust_short_text_9, cust_short_text_10,
                cust_short_text_11, cust_short_text_12, cust_long_text_1, cust_long_text_2, cust_long_text_3,
                order_instructions_set, erp_source_line_ref, erp_source_shipment_ref, erp_fulfillment_line_ref,
                min_shipping_tolerance_percentage, max_shipping_tolerance_percentage, status_id,
                order_dtl_original_seq_nbr, uom_id_id, uom_id_key, uom_id_url,
                ordered_uom_id_id, ordered_uom_id_key, ordered_uom_id_url, ordered_uom_qty,
                required_serial_nbr_set, ob_lpn_type_id, planned_parcel_shipment_nbr, orig_order_ref_id
            ) values (
                %(id)s, %(url)s, %(create_user)s, %(create_ts)s, %(mod_user)s, %(mod_ts)s,
                %(order_id_id)s, %(order_id_key)s, %(order_id_url)s, %(seq_nbr)s,
                %(item_id_id)s, %(item_id_key)s, %(item_id_url)s, %(ord_qty)s, %(orig_ord_qty)s, %(alloc_qty)s,
                %(req_cntr_nbr)s, %(po_nbr)s, %(shipment_nbr)s, %(dest_facility_attr_a)s, %(dest_facility_attr_b)s, %(dest_facility_attr_c)s,
                %(ref_nbr_1)s, %(vas_activity_code)s, %(cost)s, %(sale_price)s, %(host_ob_lpn_nbr)s, %(spl_instr)s,
                %(batch_number_id)s, %(voucher_nbr)s, %(voucher_amount)s, %(voucher_exp_date)s, %(req_pallet_nbr)s,
                %(lock_code)s, %(serial_nbr)s, %(voucher_print_count)s, %(ship_request_line)s, %(unit_declared_value)s,
                %(externally_planned_load_nbr)s, %(invn_attr_id_id)s, %(invn_attr_id_key)s, %(invn_attr_id_url)s,
                %(internal_text_field_1)s, %(orig_item_code)s, %(cust_field_1)s, %(cust_field_2)s, %(cust_field_3)s,
                %(cust_field_4)s, %(cust_field_5)s, %(cust_date_1)s, %(cust_date_2)s, %(cust_date_3)s, %(cust_date_4)s, %(cust_date_5)s,
                %(cust_number_1)s, %(cust_number_2)s, %(cust_number_3)s, %(cust_number_4)s, %(cust_number_5)s,
                %(cust_decimal_1)s, %(cust_decimal_2)s, %(cust_decimal_3)s, %(cust_decimal_4)s, %(cust_decimal_5)s,
                %(cust_short_text_1)s, %(cust_short_text_2)s, %(cust_short_text_3)s, %(cust_short_text_4)s, %(cust_short_text_5)s,
                %(cust_short_text_6)s, %(cust_short_text_7)s, %(cust_short_text_8)s, %(cust_short_text_9)s, %(cust_short_text_10)s,
                %(cust_short_text_11)s, %(cust_short_text_12)s, %(cust_long_text_1)s, %(cust_long_text_2)s, %(cust_long_text_3)s,
                %(order_instructions_set)s, %(erp_source_line_ref)s, %(erp_source_shipment_ref)s, %(erp_fulfillment_line_ref)s,
                %(min_shipping_tolerance_percentage)s, %(max_shipping_tolerance_percentage)s, %(status_id)s,
                %(order_dtl_original_seq_nbr)s, %(uom_id_id)s, %(uom_id_key)s, %(uom_id_url)s,
                %(ordered_uom_id_id)s, %(ordered_uom_id_key)s, %(ordered_uom_id_url)s, %(ordered_uom_qty)s,
                %(required_serial_nbr_set)s, %(ob_lpn_type_id)s, %(planned_parcel_shipment_nbr)s, %(orig_order_ref_id)s
            ) on conflict (id) do update set
                url = excluded.url, mod_user = excluded.mod_user, mod_ts = excluded.mod_ts,
                order_id_id = excluded.order_id_id, order_id_key = excluded.order_id_key, order_id_url = excluded.order_id_url, seq_nbr = excluded.seq_nbr,
                item_id_id = excluded.item_id_id, item_id_key = excluded.item_id_key, item_id_url = excluded.item_id_url, ord_qty = excluded.ord_qty, orig_ord_qty = excluded.orig_ord_qty, alloc_qty = excluded.alloc_qty,
                req_cntr_nbr = excluded.req_cntr_nbr, po_nbr = excluded.po_nbr, shipment_nbr = excluded.shipment_nbr, dest_facility_attr_a = excluded.dest_facility_attr_a, dest_facility_attr_b = excluded.dest_facility_attr_b, dest_facility_attr_c = excluded.dest_facility_attr_c,
                ref_nbr_1 = excluded.ref_nbr_1, vas_activity_code = excluded.vas_activity_code, cost = excluded.cost, sale_price = excluded.sale_price, host_ob_lpn_nbr = excluded.host_ob_lpn_nbr, spl_instr = excluded.spl_instr,
                batch_number_id = excluded.batch_number_id, voucher_nbr = excluded.voucher_nbr, voucher_amount = excluded.voucher_amount, voucher_exp_date = excluded.voucher_exp_date, req_pallet_nbr = excluded.req_pallet_nbr,
                lock_code = excluded.lock_code, serial_nbr = excluded.serial_nbr, voucher_print_count = excluded.voucher_print_count, ship_request_line = excluded.ship_request_line, unit_declared_value = excluded.unit_declared_value,
                externally_planned_load_nbr = excluded.externally_planned_load_nbr, invn_attr_id_id = excluded.invn_attr_id_id, invn_attr_id_key = excluded.invn_attr_id_key, invn_attr_id_url = excluded.invn_attr_id_url,
                internal_text_field_1 = excluded.internal_text_field_1, orig_item_code = excluded.orig_item_code, cust_field_1 = excluded.cust_field_1, cust_field_2 = excluded.cust_field_2, cust_field_3 = excluded.cust_field_3,
                cust_field_4 = excluded.cust_field_4, cust_field_5 = excluded.cust_field_5, cust_date_1 = excluded.cust_date_1, cust_date_2 = excluded.cust_date_2, cust_date_3 = excluded.cust_date_3, cust_date_4 = excluded.cust_date_4, cust_date_5 = excluded.cust_date_5,
                cust_number_1 = excluded.cust_number_1, cust_number_2 = excluded.cust_number_2, cust_number_3 = excluded.cust_number_3, cust_number_4 = excluded.cust_number_4, cust_number_5 = excluded.cust_number_5,
                cust_decimal_1 = excluded.cust_decimal_1, cust_decimal_2 = excluded.cust_decimal_2, cust_decimal_3 = excluded.cust_decimal_3, cust_decimal_4 = excluded.cust_decimal_4, cust_decimal_5 = excluded.cust_decimal_5,
                cust_short_text_1 = excluded.cust_short_text_1, cust_short_text_2 = excluded.cust_short_text_2, cust_short_text_3 = excluded.cust_short_text_3, cust_short_text_4 = excluded.cust_short_text_4, cust_short_text_5 = excluded.cust_short_text_5,
                cust_short_text_6 = excluded.cust_short_text_6, cust_short_text_7 = excluded.cust_short_text_7, cust_short_text_8 = excluded.cust_short_text_8, cust_short_text_9 = excluded.cust_short_text_9, cust_short_text_10 = excluded.cust_short_text_10,
                cust_short_text_11 = excluded.cust_short_text_11, cust_short_text_12 = excluded.cust_short_text_12, cust_long_text_1 = excluded.cust_long_text_1, cust_long_text_2 = excluded.cust_long_text_2, cust_long_text_3 = excluded.cust_long_text_3,
                order_instructions_set = excluded.order_instructions_set, erp_source_line_ref = excluded.erp_source_line_ref, erp_source_shipment_ref = excluded.erp_source_shipment_ref, erp_fulfillment_line_ref = excluded.erp_fulfillment_line_ref,
                min_shipping_tolerance_percentage = excluded.min_shipping_tolerance_percentage, max_shipping_tolerance_percentage = excluded.max_shipping_tolerance_percentage, status_id = excluded.status_id,
                order_dtl_original_seq_nbr = excluded.order_dtl_original_seq_nbr, uom_id_id = excluded.uom_id_id, uom_id_key = excluded.uom_id_key, uom_id_url = excluded.uom_id_url,
                ordered_uom_id_id = excluded.ordered_uom_id_id, ordered_uom_id_key = excluded.ordered_uom_id_key, ordered_uom_id_url = excluded.ordered_uom_id_url, ordered_uom_qty = excluded.ordered_uom_qty,
                required_serial_nbr_set = excluded.required_serial_nbr_set, ob_lpn_type_id = excluded.ob_lpn_type_id, planned_parcel_shipment_nbr = excluded.planned_parcel_shipment_nbr, orig_order_ref_id = excluded.orig_order_ref_id
            """,
            [
                {
                    "id": item.get("id"),
                    "url": item.get("url"),
                    "create_user": item.get("create_user"),
                    "create_ts": item.get("create_ts"),
                    "mod_user": item.get("mod_user"),
                    "mod_ts": item.get("mod_ts"),
                    "order_id_id": item.get("order_id.id"),
                    "order_id_key": item.get("order_id.key"),
                    "order_id_url": item.get("order_id.url"),
                    "seq_nbr": item.get("seq_nbr"),
                    "item_id_id": item.get("item_id.id"),
                    "item_id_key": item.get("item_id.key"),
                    "item_id_url": item.get("item_id.url"),
                    "ord_qty": item.get("ord_qty"),
                    "orig_ord_qty": item.get("orig_ord_qty"),
                    "alloc_qty": item.get("alloc_qty"),
                    "req_cntr_nbr": item.get("req_cntr_nbr"),
                    "po_nbr": item.get("po_nbr"),
                    "shipment_nbr": item.get("shipment_nbr"),
                    "dest_facility_attr_a": item.get("dest_facility_attr_a"),
                    "dest_facility_attr_b": item.get("dest_facility_attr_b"),
                    "dest_facility_attr_c": item.get("dest_facility_attr_c"),
                    "ref_nbr_1": item.get("ref_nbr_1"),
                    "vas_activity_code": item.get("vas_activity_code"),
                    "cost": item.get("cost"),
                    "sale_price": item.get("sale_price"),
                    "host_ob_lpn_nbr": item.get("host_ob_lpn_nbr"),
                    "spl_instr": item.get("spl_instr"),
                    "batch_number_id": item.get("batch_number_id"),
                    "voucher_nbr": item.get("voucher_nbr"),
                    "voucher_amount": item.get("voucher_amount"),
                    "voucher_exp_date": item.get("voucher_exp_date"),
                    "req_pallet_nbr": item.get("req_pallet_nbr"),
                    "lock_code": item.get("lock_code"),
                    "serial_nbr": item.get("serial_nbr"),
                    "voucher_print_count": item.get("voucher_print_count"),
                    "ship_request_line": item.get("ship_request_line"),
                    "unit_declared_value": item.get("unit_declared_value"),
                    "externally_planned_load_nbr": item.get(
                        "externally_planned_load_nbr"
                    ),
                    "invn_attr_id_id": item.get("invn_attr_id.id"),
                    "invn_attr_id_key": item.get("invn_attr_id.key"),
                    "invn_attr_id_url": item.get("invn_attr_id.url"),
                    "internal_text_field_1": item.get("internal_text_field_1"),
                    "orig_item_code": item.get("orig_item_code"),
                    "cust_field_1": item.get("cust_field_1"),
                    "cust_field_2": item.get("cust_field_2"),
                    "cust_field_3": item.get("cust_field_3"),
                    "cust_field_4": item.get("cust_field_4"),
                    "cust_field_5": item.get("cust_field_5"),
                    "cust_date_1": item.get("cust_date_1"),
                    "cust_date_2": item.get("cust_date_2"),
                    "cust_date_3": item.get("cust_date_3"),
                    "cust_date_4": item.get("cust_date_4"),
                    "cust_date_5": item.get("cust_date_5"),
                    "cust_number_1": item.get("cust_number_1"),
                    "cust_number_2": item.get("cust_number_2"),
                    "cust_number_3": item.get("cust_number_3"),
                    "cust_number_4": item.get("cust_number_4"),
                    "cust_number_5": item.get("cust_number_5"),
                    "cust_decimal_1": item.get("cust_decimal_1"),
                    "cust_decimal_2": item.get("cust_decimal_2"),
                    "cust_decimal_3": item.get("cust_decimal_3"),
                    "cust_decimal_4": item.get("cust_decimal_4"),
                    "cust_decimal_5": item.get("cust_decimal_5"),
                    "cust_short_text_1": item.get("cust_short_text_1"),
                    "cust_short_text_2": item.get("cust_short_text_2"),
                    "cust_short_text_3": item.get("cust_short_text_3"),
                    "cust_short_text_4": item.get("cust_short_text_4"),
                    "cust_short_text_5": item.get("cust_short_text_5"),
                    "cust_short_text_6": item.get("cust_short_text_6"),
                    "cust_short_text_7": item.get("cust_short_text_7"),
                    "cust_short_text_8": item.get("cust_short_text_8"),
                    "cust_short_text_9": item.get("cust_short_text_9"),
                    "cust_short_text_10": item.get("cust_short_text_10"),
                    "cust_short_text_11": item.get("cust_short_text_11"),
                    "cust_short_text_12": item.get("cust_short_text_12"),
                    "cust_long_text_1": item.get("cust_long_text_1"),
                    "cust_long_text_2": item.get("cust_long_text_2"),
                    "cust_long_text_3": item.get("cust_long_text_3"),
                    "order_instructions_set": item.get("order_instructions_set"),
                    "erp_source_line_ref": item.get("erp_source_line_ref"),
                    "erp_source_shipment_ref": item.get("erp_source_shipment_ref"),
                    "erp_fulfillment_line_ref": item.get("erp_fulfillment_line_ref"),
                    "min_shipping_tolerance_percentage": item.get(
                        "min_shipping_tolerance_percentage"
                    ),
                    "max_shipping_tolerance_percentage": item.get(
                        "max_shipping_tolerance_percentage"
                    ),
                    "status_id": item.get("status_id"),
                    "order_dtl_original_seq_nbr": item.get(
                        "order_dtl_original_seq_nbr"
                    ),
                    "uom_id_id": item.get("uom_id.id"),
                    "uom_id_key": item.get("uom_id.key"),
                    "uom_id_url": item.get("uom_id.url"),
                    "ordered_uom_id_id": item.get("ordered_uom_id.id"),
                    "ordered_uom_id_key": item.get("ordered_uom_id.key"),
                    "ordered_uom_id_url": item.get("ordered_uom_id.url"),
                    "ordered_uom_qty": item.get("ordered_uom_qty"),
                    "required_serial_nbr_set": item.get("required_serial_nbr_set"),
                    "ob_lpn_type_id": item.get("ob_lpn_type_id"),
                    "planned_parcel_shipment_nbr": item.get(
                        "planned_parcel_shipment_nbr"
                    ),
                    "orig_order_ref_id": item.get("orig_order_ref_id"),
                }
                for item in items
            ],
        )
    conn.commit()
    return len(items)


def upsert_order_status(
    conn: psycopg.Connection, rows: Iterable[Dict[str, Any]]
) -> int:
    items: List[Dict[str, Any]] = list(rows)
    if not items:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.raw_order_status (
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


def upsert_location(conn: psycopg.Connection, rows: Iterable[Dict[str, Any]]) -> int:
    items: List[Dict[str, Any]] = list(rows)
    if not items:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.raw_location (
                id, url, create_user, create_ts, mod_user, mod_ts,
                facility_id_id, facility_id_key, facility_id_url,
                dedicated_company_id_id, dedicated_company_id_key, dedicated_company_id_url,
                area, aisle, bay, level, position, bin,
                type_id_id, type_id_key, type_id_url,
                allow_multi_sku, barcode, destination_company_id_id,
                length, width, height, max_units, max_lpns, to_be_counted_flg, to_be_counted_ts,
                lock_code_id, lock_for_putaway_flg, pick_seq, last_count_ts, last_count_user,
                locn_size_type_id, min_units, allow_reserve_partial_pick_flg, alloc_zone, locn_str, putaway_seq,
                replenishment_zone_id_id, replenishment_zone_id_key, replenishment_zone_id_url,
                min_volume, max_volume, restrict_batch_nbr_flg, item_assignment_type_id_id, item_assignment_type_id_key, item_assignment_type_id_url,
                item_id_id, item_id_key, item_id_url, mhe_system_id, pick_zone, divert_lane, task_zone_id,
                in_transit_units, restrict_invn_attr_flg, assembly_flg, billing_location_type,
                cust_field_1, cust_field_2, cust_field_3, cust_field_4, cust_field_5,
                min_weight, max_weight, cc_threshold_uom_id_id, cc_threshold_uom_id_key, cc_threshold_uom_id_url,
                cc_threshold_value, x_coordinate, y_coordinate, z_coordinate, lock_applied_ts,
                ignore_attr_values_for_restrict_invn_attr, ranking
            ) values (
                %(id)s, %(url)s, %(create_user)s, %(create_ts)s, %(mod_user)s, %(mod_ts)s,
                %(facility_id_id)s, %(facility_id_key)s, %(facility_id_url)s,
                %(dedicated_company_id_id)s, %(dedicated_company_id_key)s, %(dedicated_company_id_url)s,
                %(area)s, %(aisle)s, %(bay)s, %(level)s, %(position)s, %(bin)s,
                %(type_id_id)s, %(type_id_key)s, %(type_id_url)s,
                %(allow_multi_sku)s, %(barcode)s, %(destination_company_id_id)s,
                %(length)s, %(width)s, %(height)s, %(max_units)s, %(max_lpns)s, %(to_be_counted_flg)s, %(to_be_counted_ts)s,
                %(lock_code_id)s, %(lock_for_putaway_flg)s, %(pick_seq)s, %(last_count_ts)s, %(last_count_user)s,
                %(locn_size_type_id)s, %(min_units)s, %(allow_reserve_partial_pick_flg)s, %(alloc_zone)s, %(locn_str)s, %(putaway_seq)s,
                %(replenishment_zone_id_id)s, %(replenishment_zone_id_key)s, %(replenishment_zone_id_url)s,
                %(min_volume)s, %(max_volume)s, %(restrict_batch_nbr_flg)s, %(item_assignment_type_id_id)s, %(item_assignment_type_id_key)s, %(item_assignment_type_id_url)s,
                %(item_id_id)s, %(item_id_key)s, %(item_id_url)s, %(mhe_system_id)s, %(pick_zone)s, %(divert_lane)s, %(task_zone_id)s,
                %(in_transit_units)s, %(restrict_invn_attr_flg)s, %(assembly_flg)s, %(billing_location_type)s,
                %(cust_field_1)s, %(cust_field_2)s, %(cust_field_3)s, %(cust_field_4)s, %(cust_field_5)s,
                %(min_weight)s, %(max_weight)s, %(cc_threshold_uom_id_id)s, %(cc_threshold_uom_id_key)s, %(cc_threshold_uom_id_url)s,
                %(cc_threshold_value)s, %(x_coordinate)s, %(y_coordinate)s, %(z_coordinate)s, %(lock_applied_ts)s,
                %(ignore_attr_values_for_restrict_invn_attr)s, %(ranking)s
            ) on conflict (id) do update set
                url = excluded.url, mod_user = excluded.mod_user, mod_ts = excluded.mod_ts,
                facility_id_id = excluded.facility_id_id, facility_id_key = excluded.facility_id_key, facility_id_url = excluded.facility_id_url,
                dedicated_company_id_id = excluded.dedicated_company_id_id, dedicated_company_id_key = excluded.dedicated_company_id_key, dedicated_company_id_url = excluded.dedicated_company_id_url,
                area = excluded.area, aisle = excluded.aisle, bay = excluded.bay, level = excluded.level, position = excluded.position, bin = excluded.bin,
                type_id_id = excluded.type_id_id, type_id_key = excluded.type_id_key, type_id_url = excluded.type_id_url,
                allow_multi_sku = excluded.allow_multi_sku, barcode = excluded.barcode, destination_company_id_id = excluded.destination_company_id_id,
                length = excluded.length, width = excluded.width, height = excluded.height, max_units = excluded.max_units, max_lpns = excluded.max_lpns, to_be_counted_flg = excluded.to_be_counted_flg, to_be_counted_ts = excluded.to_be_counted_ts,
                lock_code_id = excluded.lock_code_id, lock_for_putaway_flg = excluded.lock_for_putaway_flg, pick_seq = excluded.pick_seq, last_count_ts = excluded.last_count_ts, last_count_user = excluded.last_count_user,
                locn_size_type_id = excluded.locn_size_type_id, min_units = excluded.min_units, allow_reserve_partial_pick_flg = excluded.allow_reserve_partial_pick_flg, alloc_zone = excluded.alloc_zone, locn_str = excluded.locn_str, putaway_seq = excluded.putaway_seq,
                replenishment_zone_id_id = excluded.replenishment_zone_id_id, replenishment_zone_id_key = excluded.replenishment_zone_id_key, replenishment_zone_id_url = excluded.replenishment_zone_id_url,
                min_volume = excluded.min_volume, max_volume = excluded.max_volume, restrict_batch_nbr_flg = excluded.restrict_batch_nbr_flg, item_assignment_type_id_id = excluded.item_assignment_type_id_id, item_assignment_type_id_key = excluded.item_assignment_type_id_key, item_assignment_type_id_url = excluded.item_assignment_type_id_url,
                item_id_id = excluded.item_id_id, item_id_key = excluded.item_id_key, item_id_url = excluded.item_id_url, mhe_system_id = excluded.mhe_system_id, pick_zone = excluded.pick_zone, divert_lane = excluded.divert_lane, task_zone_id = excluded.task_zone_id,
                in_transit_units = excluded.in_transit_units, restrict_invn_attr_flg = excluded.restrict_invn_attr_flg, assembly_flg = excluded.assembly_flg, billing_location_type = excluded.billing_location_type,
                cust_field_1 = excluded.cust_field_1, cust_field_2 = excluded.cust_field_2, cust_field_3 = excluded.cust_field_3, cust_field_4 = excluded.cust_field_4, cust_field_5 = excluded.cust_field_5,
                min_weight = excluded.min_weight, max_weight = excluded.max_weight, cc_threshold_uom_id_id = excluded.cc_threshold_uom_id_id, cc_threshold_uom_id_key = excluded.cc_threshold_uom_id_key, cc_threshold_uom_id_url = excluded.cc_threshold_uom_id_url,
                cc_threshold_value = excluded.cc_threshold_value, x_coordinate = excluded.x_coordinate, y_coordinate = excluded.y_coordinate, z_coordinate = excluded.z_coordinate, lock_applied_ts = excluded.lock_applied_ts,
                ignore_attr_values_for_restrict_invn_attr = excluded.ignore_attr_values_for_restrict_invn_attr, ranking = excluded.ranking
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
                    "dedicated_company_id_id": item.get("dedicated_company_id.id"),
                    "dedicated_company_id_key": item.get("dedicated_company_id.key"),
                    "dedicated_company_id_url": item.get("dedicated_company_id.url"),
                    "area": item.get("area") if item.get("area") else None,
                    "aisle": item.get("aisle") if item.get("aisle") else None,
                    "bay": item.get("bay") if item.get("bay") else None,
                    "level": item.get("level") if item.get("level") else None,
                    "position": item.get("position") if item.get("position") else None,
                    "bin": item.get("bin") if item.get("bin") else None,
                    "type_id_id": item.get("type_id.id"),
                    "type_id_key": item.get("type_id.key"),
                    "type_id_url": item.get("type_id.url"),
                    "allow_multi_sku": item.get("allow_multi_sku")
                    if item.get("allow_multi_sku") is not None
                    else None,
                    "barcode": item.get("barcode") if item.get("barcode") else None,
                    "destination_company_id_id": item.get("destination_company_id.id")
                    if item.get("destination_company_id")
                    else None,
                    "length": item.get("length"),
                    "width": item.get("width"),
                    "height": item.get("height"),
                    "max_units": item.get("max_units")
                    if item.get("max_units")
                    else None,
                    "max_lpns": item.get("max_lpns") if item.get("max_lpns") else None,
                    "to_be_counted_flg": item.get("to_be_counted_flg")
                    if item.get("to_be_counted_flg") is not None
                    else None,
                    "to_be_counted_ts": item.get("to_be_counted_ts")
                    if item.get("to_be_counted_ts")
                    else None,
                    "lock_code_id": item.get("lock_code_id")
                    if item.get("lock_code_id")
                    else None,
                    "lock_for_putaway_flg": item.get("lock_for_putaway_flg")
                    if item.get("lock_for_putaway_flg") is not None
                    else None,
                    "pick_seq": item.get("pick_seq") if item.get("pick_seq") else None,
                    "last_count_ts": item.get("last_count_ts")
                    if item.get("last_count_ts")
                    else None,
                    "last_count_user": item.get("last_count_user")
                    if item.get("last_count_user")
                    else None,
                    "locn_size_type_id": item.get("locn_size_type_id")
                    if item.get("locn_size_type_id")
                    else None,
                    "min_units": item.get("min_units")
                    if item.get("min_units")
                    else None,
                    "allow_reserve_partial_pick_flg": item.get(
                        "allow_reserve_partial_pick_flg"
                    )
                    if item.get("allow_reserve_partial_pick_flg") is not None
                    else None,
                    "alloc_zone": item.get("alloc_zone")
                    if item.get("alloc_zone")
                    else None,
                    "locn_str": item.get("locn_str") if item.get("locn_str") else None,
                    "putaway_seq": item.get("putaway_seq")
                    if item.get("putaway_seq")
                    else None,
                    "replenishment_zone_id_id": item.get("replenishment_zone_id.id"),
                    "replenishment_zone_id_key": item.get("replenishment_zone_id.key"),
                    "replenishment_zone_id_url": item.get("replenishment_zone_id.url"),
                    "min_volume": item.get("min_volume")
                    if item.get("min_volume")
                    else None,
                    "max_volume": item.get("max_volume")
                    if item.get("max_volume")
                    else None,
                    "restrict_batch_nbr_flg": item.get("restrict_batch_nbr_flg")
                    if item.get("restrict_batch_nbr_flg") is not None
                    else None,
                    "item_assignment_type_id_id": item.get(
                        "item_assignment_type_id.id"
                    ),
                    "item_assignment_type_id_key": item.get(
                        "item_assignment_type_id.key"
                    ),
                    "item_assignment_type_id_url": item.get(
                        "item_assignment_type_id.url"
                    ),
                    "item_id_id": item.get("item_id.id"),
                    "item_id_key": item.get("item_id.key"),
                    "item_id_url": item.get("item_id.url"),
                    "mhe_system_id": item.get("mhe_system_id")
                    if item.get("mhe_system_id")
                    else None,
                    "pick_zone": item.get("pick_zone")
                    if item.get("pick_zone")
                    else None,
                    "divert_lane": item.get("divert_lane")
                    if item.get("divert_lane")
                    else None,
                    "task_zone_id": item.get("task_zone_id")
                    if item.get("task_zone_id")
                    else None,
                    "in_transit_units": item.get("in_transit_units")
                    if item.get("in_transit_units")
                    else None,
                    "restrict_invn_attr_flg": item.get("restrict_invn_attr_flg")
                    if item.get("restrict_invn_attr_flg") is not None
                    else None,
                    "assembly_flg": item.get("assembly_flg")
                    if item.get("assembly_flg") is not None
                    else None,
                    "billing_location_type": item.get("billing_location_type")
                    if item.get("billing_location_type")
                    else None,
                    "cust_field_1": item.get("cust_field_1"),
                    "cust_field_2": item.get("cust_field_2"),
                    "cust_field_3": item.get("cust_field_3"),
                    "cust_field_4": item.get("cust_field_4"),
                    "cust_field_5": item.get("cust_field_5"),
                    "min_weight": item.get("min_weight")
                    if item.get("min_weight")
                    else None,
                    "max_weight": item.get("max_weight")
                    if item.get("max_weight")
                    else None,
                    "cc_threshold_uom_id_id": item.get("cc_threshold_uom_id.id")
                    if item.get("cc_threshold_uom_id")
                    else None,
                    "cc_threshold_uom_id_key": item.get("cc_threshold_uom_id.key")
                    if item.get("cc_threshold_uom_id")
                    else None,
                    "cc_threshold_uom_id_url": item.get("cc_threshold_uom_id.url")
                    if item.get("cc_threshold_uom_id")
                    else None,
                    "cc_threshold_value": item.get("cc_threshold_value")
                    if item.get("cc_threshold_value")
                    else None,
                    "x_coordinate": item.get("x_coordinate")
                    if item.get("x_coordinate")
                    else None,
                    "y_coordinate": item.get("y_coordinate")
                    if item.get("y_coordinate")
                    else None,
                    "z_coordinate": item.get("z_coordinate")
                    if item.get("z_coordinate")
                    else None,
                    "lock_applied_ts": item.get("lock_applied_ts")
                    if item.get("lock_applied_ts")
                    else None,
                    "ignore_attr_values_for_restrict_invn_attr": item.get(
                        "ignore_attr_values_for_restrict_invn_attr"
                    )
                    if item.get("ignore_attr_values_for_restrict_invn_attr")
                    else None,
                    "ranking": item.get("ranking") if item.get("ranking") else None,
                }
                for item in items
            ],
        )
    conn.commit()
    return len(items)
