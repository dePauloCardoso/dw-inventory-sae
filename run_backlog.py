"""
run_backlog.py
Extrai backlog completo do WMS Oracle Cloud em batches e exporta CSVs compatíveis com dbt seed.
"""

from __future__ import annotations
import os
import csv
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from wms_client import WMSClient
from db import get_connection
from extractors.inventory import _flatten_inventory_record
from extractors.container import _flatten_container_record
from extractors.location import _flatten_location_record
from extractors.order_hdr import _flatten_order_hdr_record
from extractors.order_dtl import _flatten_order_dtl_record

# -----------------------------------------------------------------------------
# CONFIGURAÇÃO
# -----------------------------------------------------------------------------
START_DATE = datetime(2025, 9, 1)
END_DATE = datetime.now()
PAGE_BATCH_SIZE = 500
OUTPUT_DIR = "backlog_exports"

# -----------------------------------------------------------------------------
# COLUNAS COMPLETAS DE CADA TABELA
# -----------------------------------------------------------------------------
TABLE_COLUMNS = {
    "inventory": [
        "id",
        "url",
        "create_user",
        "create_ts",
        "mod_user",
        "mod_ts",
        "facility_id_id",
        "facility_id_key",
        "facility_id_url",
        "item_id_id",
        "item_id_key",
        "item_id_url",
        "location_id",
        "location_id_id",
        "location_id_key",
        "location_id_url",
        "container_id",
        "container_id_id",
        "container_id_key",
        "container_id_url",
        "priority_date",
        "curr_qty",
        "orig_qty",
        "pack_qty",
        "case_qty",
        "status_id",
        "manufacture_date",
        "expiry_date",
        "batch_number_id",
        "invn_attr_id_id",
        "invn_attr_id_key",
        "invn_attr_id_url",
        "serial_nbr_set",
        "uom_id_id",
        "uom_id_key",
        "uom_id_url",
    ],
    "container": [
        "id",
        "container_nbr",
        "url",
        "create_user",
        "create_ts",
        "mod_user",
        "mod_ts",
        "facility_id_id",
        "facility_id_key",
        "facility_id_url",
        "company_id_id",
        "company_id_key",
        "company_id_url",
        "status_id",
        "vas_status_id",
        "curr_location_id_id",
        "curr_location_id_key",
        "curr_location_id_url",
        "prev_location_id",
        "prev_location_id_id",
        "prev_location_id_key",
        "prev_location_id_url",
        "rcvd_shipment_id_id",
        "rcvd_shipment_id_key",
        "rcvd_shipment_id_url",
        "putawaytype_id_id",
        "putawaytype_id_key",
        "putawaytype_id_url",
        "pallet_id",
        "lpn_type_id",
        "asset_id",
        "weight",
        "volume",
        "length",
        "width",
        "height",
        "parcel_batch_flg",
        "price_labels_printed",
        "actual_weight_flg",
        "rcvd_ts",
        "first_putaway_ts",
        "priority_date",
        "audit_status_id",
        "qc_status_id",
        "cart_posn_nbr",
        "nbr_files",
        "type",
        "rcvd_user",
        "pick_user",
        "pack_user",
        "ref_iblpn_nbr",
        "ref_shipment_nbr",
        "ref_po_nbr",
        "ref_oblpn_nbr",
        "asset_seal_nbr",
        "comments",
        "rcvd_trailer_nbr",
        "orig_container_nbr",
        "pallet_position",
        "inventory_lock_set_url",
        "inventory_lock_set_result_count",
        "cust_field_1",
        "cust_field_2",
        "cust_field_3",
        "cust_field_4",
        "cust_field_5",
        "cart_nbr",
    ],
    "location": [
        "id",
        "url",
        "create_user",
        "create_ts",
        "mod_user",
        "mod_ts",
        "facility_id_id",
        "facility_id_key",
        "facility_id_url",
        "dedicated_company_id_id",
        "dedicated_company_id_key",
        "dedicated_company_id_url",
        "area",
        "aisle",
        "bay",
        "level",
        "position",
        "bin",
        "type_id_id",
        "type_id_key",
        "type_id_url",
        "allow_multi_sku",
        "barcode",
        "destination_company_id_id",
        "length",
        "width",
        "height",
        "max_units",
        "max_lpns",
        "to_be_counted_flg",
        "to_be_counted_ts",
        "lock_code_id",
        "lock_for_putaway_flg",
        "pick_seq",
        "last_count_ts",
        "last_count_user",
        "locn_size_type_id",
        "min_units",
        "allow_reserve_partial_pick_flg",
        "alloc_zone",
        "locn_str",
        "putaway_seq",
        "replenishment_zone_id_id",
        "replenishment_zone_id_key",
        "replenishment_zone_id_url",
        "min_volume",
        "max_volume",
        "restrict_batch_nbr_flg",
        "item_assignment_type_id_id",
        "item_assignment_type_id_key",
        "item_assignment_type_id_url",
        "item_id_id",
        "item_id_key",
        "item_id_url",
        "mhe_system_id",
        "pick_zone",
        "divert_lane",
        "task_zone_id",
        "in_transit_units",
        "restrict_invn_attr_flg",
        "assembly_flg",
        "billing_location_type",
        "cust_field_1",
        "cust_field_2",
        "cust_field_3",
        "cust_field_4",
        "cust_field_5",
        "min_weight",
        "max_weight",
        "cc_threshold_uom_id_id",
        "cc_threshold_uom_id_key",
        "cc_threshold_uom_id_url",
        "cc_threshold_value",
        "x_coordinate",
        "y_coordinate",
        "z_coordinate",
        "lock_applied_ts",
        "ignore_attr_values_for_restrict_invn_attr",
        "ranking",
        "destination_company_id",
        "cc_threshold_uom_id",
    ],
    "order_hdr": [
        "id",
        "url",
        "create_user",
        "create_ts",
        "mod_user",
        "mod_ts",
        "facility_id_id",
        "facility_id_key",
        "facility_id_url",
        "company_id_id",
        "company_id_key",
        "company_id_url",
        "order_nbr",
        "order_type_id_id",
        "order_type_id_key",
        "order_type_id_url",
        "status_id",
        "ord_date",
        "exp_date",
        "req_ship_date",
        "dest_facility_id",
        "shipto_facility_id",
        "cust_name",
        "cust_addr",
        "cust_addr2",
        "cust_addr3",
        "cust_city",
        "cust_state",
        "cust_zip",
        "cust_country",
        "cust_phone_nbr",
        "cust_email",
        "cust_nbr",
        "shipto_name",
        "shipto_addr",
        "shipto_addr2",
        "shipto_addr3",
        "shipto_city",
        "shipto_state",
        "shipto_zip",
        "shipto_country",
        "shipto_phone_nbr",
        "shipto_email",
        "ref_nbr",
        "stage_location_id",
        "ship_via_ref_code",
        "route_nbr",
        "external_route",
        "destination_company_id_id",
        "destination_company_id_key",
        "destination_company_id_url",
        "ship_via_id",
        "priority",
        "host_allocation_nbr",
        "sales_order_nbr",
        "sales_channel",
        "customer_po_nbr",
        "carrier_account_nbr",
        "payment_method_id",
        "dest_dept_nbr",
        "start_ship_date",
        "stop_ship_date",
        "vas_group_code",
        "spl_instr",
        "currency_code",
        "record_origin_code",
        "cust_contact",
        "shipto_contact",
        "ob_lpn_type",
        "ob_lpn_type_id",
        "total_orig_ord_qty",
        "orig_sku_count",
        "orig_sale_price",
        "gift_msg",
        "sched_ship_date",
        "customer_po_type",
        "customer_vendor_code",
        "externally_planned_load_flg",
        "work_order_kit_id",
        "order_nbr_to_replace",
        "stop_ship_flg",
        "lpn_type_class",
        "billto_carrier_account_nbr",
        "duties_carrier_account_nbr",
        "duties_payment_method_id",
        "customs_broker_contact_id",
        "order_shipped_ts",
        "cust_field_1",
        "cust_field_2",
        "cust_field_3",
        "cust_field_4",
        "cust_field_5",
        "cust_date_1",
        "cust_date_2",
        "cust_date_3",
        "cust_date_4",
        "cust_date_5",
        "cust_number_1",
        "cust_number_2",
        "cust_number_3",
        "cust_number_4",
        "cust_number_5",
        "cust_decimal_1",
        "cust_decimal_2",
        "cust_decimal_3",
        "cust_decimal_4",
        "cust_decimal_5",
        "cust_short_text_1",
        "cust_short_text_2",
        "cust_short_text_3",
        "cust_short_text_4",
        "cust_short_text_5",
        "cust_short_text_6",
        "cust_short_text_7",
        "cust_short_text_8",
        "cust_short_text_9",
        "cust_short_text_10",
        "cust_short_text_11",
        "cust_short_text_12",
        "cust_long_text_1",
        "cust_long_text_2",
        "cust_long_text_3",
        "order_instructions_set",
        "order_dtl_set_result_count",
        "order_dtl_set_url",
        "order_lock_set",
        "tms_parcel_shipment_nbr",
        "erp_source_hdr_ref",
        "erp_source_system_ref",
        "tms_order_hdr_ref",
        "group_ref",
    ],
    "order_dtl": [
        "id",
        "url",
        "create_user",
        "create_ts",
        "mod_user",
        "mod_ts",
        "order_id_id",
        "order_id_key",
        "order_id_url",
        "seq_nbr",
        "item_id_id",
        "item_id_key",
        "item_id_url",
        "ord_qty",
        "orig_ord_qty",
        "alloc_qty",
        "req_cntr_nbr",
        "po_nbr",
        "shipment_nbr",
        "dest_facility_attr_a",
        "dest_facility_attr_b",
        "dest_facility_attr_c",
        "ref_nbr_1",
        "vas_activity_code",
        "cost",
        "sale_price",
        "host_ob_lpn_nbr",
        "spl_instr",
        "batch_number_id",
        "voucher_nbr",
        "voucher_amount",
        "voucher_exp_date",
        "req_pallet_nbr",
        "lock_code",
        "serial_nbr",
        "voucher_print_count",
        "ship_request_line",
        "unit_declared_value",
        "externally_planned_load_nbr",
        "invn_attr_id_id",
        "invn_attr_id_key",
        "invn_attr_id_url",
        "internal_text_field_1",
        "orig_item_code",
        "cust_field_1",
        "cust_field_2",
        "cust_field_3",
        "cust_field_4",
        "cust_field_5",
        "cust_date_1",
        "cust_date_2",
        "cust_date_3",
        "cust_date_4",
        "cust_date_5",
        "cust_number_1",
        "cust_number_2",
        "cust_number_3",
        "cust_number_4",
        "cust_number_5",
        "cust_decimal_1",
        "cust_decimal_2",
        "cust_decimal_3",
        "cust_decimal_4",
        "cust_decimal_5",
        "cust_short_text_1",
        "cust_short_text_2",
        "cust_short_text_3",
        "cust_short_text_4",
        "cust_short_text_5",
        "cust_short_text_6",
        "cust_short_text_7",
        "cust_short_text_8",
        "cust_short_text_9",
        "cust_short_text_10",
        "cust_short_text_11",
        "cust_short_text_12",
        "cust_long_text_1",
        "cust_long_text_2",
        "cust_long_text_3",
        "order_instructions_set",
        "erp_source_line_ref",
        "erp_source_shipment_ref",
        "erp_fulfillment_line_ref",
        "min_shipping_tolerance_percentage",
        "max_shipping_tolerance_percentage",
        "status_id",
        "order_dtl_original_seq_nbr",
        "uom_id_id",
        "uom_id_key",
        "uom_id_url",
        "ordered_uom_id_id",
        "ordered_uom_id_key",
        "ordered_uom_id_url",
        "ordered_uom_qty",
        "required_serial_nbr_set",
        "ob_lpn_type_id",
        "planned_parcel_shipment_nbr",
        "orig_order_ref_id",
    ],
}

# -----------------------------------------------------------------------------
# LOGGER
# -----------------------------------------------------------------------------
logger = logging.getLogger("wms_backlog")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


# -----------------------------------------------------------------------------
# CORE FUNCTIONS
# -----------------------------------------------------------------------------
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def export_to_csv(table: str, records: List[Dict[str, Any]]):
    ensure_dir(OUTPUT_DIR)
    filename = os.path.join(OUTPUT_DIR, f"{table}.csv")
    cols = TABLE_COLUMNS[table]
    file_exists = os.path.exists(filename)

    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        if not file_exists:
            writer.writeheader()
        for rec in records:
            row = {k: rec.get(k) for k in cols}
            writer.writerow(row)


def fetch_backlog(
    client: WMSClient, entity: str, flattener, start: datetime, end: datetime
):
    logger.info(f"==> Extraindo backlog de {entity} de {start.date()} até {end.date()}")
    current = start
    total_records = 0

    while current < end:
        next_day = current + timedelta(days=1)
        params = {
            "create_ts__gte": current.strftime("%Y-%m-%dT00:00:00"),
            "create_ts__lt": next_day.strftime("%Y-%m-%dT00:00:00"),
        }

        try:
            results = client.fetch_all_sync(entity, params=params)  # sem page_size
            if not results:
                logger.info(f"[{entity}] Nenhum dado em {current.date()}")
                current = next_day
                continue

            flattened = [flattener(x) for x in results]
            export_to_csv(entity, flattened)
            total_records += len(flattened)
            logger.info(
                f"[{entity}] {current.date()} → {len(results)} registros exportados"
            )

        except Exception as e:
            if "404" in str(e):
                logger.warning(
                    f"[{entity}] Nenhum dado encontrado ({current.date()}) (404)"
                )
            else:
                logger.error(f"[{entity}] Erro em {current.date()}: {e}")

        current = next_day

    logger.info(f"[{entity}] Total exportado: {total_records} registros")


def main():
    logger.info("Iniciando extração de backlog...")
    ensure_dir(OUTPUT_DIR)

    client = WMSClient()
    conn = get_connection()  # noqa: F841

    extractors = {
        "inventory": _flatten_inventory_record,
        "container": _flatten_container_record,
        "location": _flatten_location_record,
        "order_hdr": _flatten_order_hdr_record,
        "order_dtl": _flatten_order_dtl_record,
    }

    for entity, flattener in extractors.items():
        try:
            fetch_backlog(client, entity, flattener, START_DATE, END_DATE)
        except Exception as e:
            logger.error(f"Erro ao processar {entity}: {e}")

    logger.info(
        "✅ Backlog exportado com sucesso! CSVs disponíveis em ./backlog_exports/"
    )


if __name__ == "__main__":
    main()
