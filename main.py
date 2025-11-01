# main.py
from __future__ import annotations
import logging
import sys

from db import get_connection
from wms_client import WMSClient
from extractors.inventory import extract_and_upsert_inventory
from extractors.order_hdr import extract_and_upsert_order_hdr
from extractors.order_dtl import extract_and_upsert_order_dtl
from extractors.container import extract_and_upsert_container
from extractors.location import extract_and_upsert_location
from extractors.oblpn import extract_and_upsert_oblpn

# se você tiver outros extractors, importe aqui (ordem e nome iguais ao seu projeto original)
# from extractors.container import extract_and_upsert_container
# ...

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("wms_etl")


def main() -> None:
    try:
        logger.info("Starting WMS data extraction")

        conn = get_connection()
        with WMSClient() as client:
            # inventory
            logger.info("Extracting inventory data...")
            inv_count = extract_and_upsert_inventory(client, conn)
            logger.info("Inventory processed: %d", inv_count)

            logger.info("Extracting order_hdr data...")
            hdr_count = extract_and_upsert_order_hdr(client, conn)
            logger.info("Order_hdr processed: %d", hdr_count)

            logger.info("Extracting order_dtl data...")
            dtl_count = extract_and_upsert_order_dtl(client, conn)
            logger.info("Order_dtl processed: %d", dtl_count)

            logger.info("Extracting container data...")
            cont_count = extract_and_upsert_container(client, conn)
            logger.info("Container processed: %d", cont_count)

            logger.info("Extracting location data...")
            loc_count = extract_and_upsert_location(client, conn)
            logger.info("Location processed: %d", loc_count)

            logger.info("Extracting oblpn data...")
            loc_count = extract_and_upsert_oblpn(client, conn)
            logger.info("Oblpn processed: %d", loc_count)

            # outros extractors (exemplo)
            # logger.info("Extracting container data...")
            # cont_count = extract_and_upsert_container(client, conn)
            # logger.info("Container processed: %d", cont_count)

        conn.close()
        logger.info("Extraction finished successfully")

    except Exception:
        logger.exception("Fatal error during extraction")
        raise


if __name__ == "__main__":
    main()
