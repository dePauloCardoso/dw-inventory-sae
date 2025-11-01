"""
run_backlog.py
Extrai backlog completo do WMS Oracle Cloud em batches e exporta CSVs compatíveis com dbt seed.
Pode-se comentar linhas no dicionário ACTIVE_EXTRACTORS para controlar quais entidades serão extraídas.
"""

from __future__ import annotations
import os
import csv
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from wms_client import WMSClient
from db import get_connection
from extractors.oblpn import _flatten_oblpn_record  # ✅ novo import

# -----------------------------------------------------------------------------
# CONFIGURAÇÃO
# -----------------------------------------------------------------------------
START_DATE = datetime(2025, 9, 1)
END_DATE = datetime.now()
PAGE_BATCH_SIZE = 500
OUTPUT_DIR = "backlog_exports"

# -----------------------------------------------------------------------------
# ATIVE OU COMENTE OS EXTRATORES QUE QUISER RODAR
# -----------------------------------------------------------------------------
ACTIVE_EXTRACTORS = {
    # "inventory": _flatten_inventory_record,
    # "container": _flatten_container_record,
    # "location": _flatten_location_record,
    # "order_hdr": _flatten_order_hdr_record,
    # "order_dtl": _flatten_order_dtl_record,
    "oblpn": _flatten_oblpn_record,  # ✅ pode comentar/descomentar facilmente
}

# -----------------------------------------------------------------------------
# COLUNAS COMPLETAS DE CADA TABELA
# -----------------------------------------------------------------------------
TABLE_COLUMNS = {
    "inventory": [...],  # já existente no seu script
    "container": [...],
    "location": [...],
    "order_hdr": [...],
    "order_dtl": [...],
    "oblpn": [
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
        "curr_location_id_id",
        "curr_location_id_key",
        "curr_location_id_url",
        "prev_location_id_id",
        "prev_location_id_key",
        "prev_location_id_url",
        "container_nbr",
        "type",
        "status_id",
        "vas_status_id",
        "priority_date",
        "pallet_id",
        "rcvd_shipment_id",
        "rcvd_ts",
        "rcvd_user",
        "weight",
        "volume",
        "pick_user",
        "pack_user",
        "putawaytype_id",
        "ref_iblpn_nbr",
        "ref_shipment_nbr",
        "ref_po_nbr",
        "ref_oblpn_nbr",
        "first_putaway_ts",
        "parcel_batch_flg",
        "lpn_type_id",
        "cart_posn_nbr",
        "audit_status_id",
        "qc_status_id",
        "asset_id",
        "asset_seal_nbr",
        "price_labels_printed",
        "comments",
        "actual_weight_flg",
        "length",
        "width",
        "height",
        "rcvd_trailer_nbr",
        "orig_container_nbr",
        "pallet_position",
        "inventory_lock_set",
        "nbr_files",
        "cust_field_1",
        "cust_field_2",
        "cust_field_3",
        "cust_field_4",
        "cust_field_5",
        "cart_nbr",
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
            results = client.fetch_all_sync(entity, params=params)
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

    for entity, flattener in ACTIVE_EXTRACTORS.items():
        try:
            fetch_backlog(client, entity, flattener, START_DATE, END_DATE)
        except Exception as e:
            logger.error(f"Erro ao processar {entity}: {e}")

    logger.info(
        "✅ Backlog exportado com sucesso! CSVs disponíveis em ./backlog_exports/"
    )


if __name__ == "__main__":
    main()
