from __future__ import annotations

from db import get_connection
from wms_client import WMSClient
from extractors.inventory import extract_and_upsert_inventory
from extractors.container import extract_and_upsert_container
from extractors.container_status import extract_and_upsert_container_status
from extractors.location import extract_and_upsert_location
from extractors.order_hdr import extract_and_upsert_order_hdr
from extractors.order_dtl import extract_and_upsert_order_dtl
from extractors.order_status import extract_and_upsert_order_status


def main() -> None:
    try:
        print("Starting WMS data extraction...")

        with get_connection() as conn:
            print("Initializing WMS client...")
            with WMSClient() as client:
                print("Extracting inventory data...")
                inv_count = extract_and_upsert_inventory(client, conn)
                print(f"Inventory: {inv_count} records processed")

                print("Extracting container data...")
                cont_count = extract_and_upsert_container(client, conn)
                print(f"Container: {cont_count} records processed")

                print("Extracting container status data...")
                status_count = extract_and_upsert_container_status(client, conn)
                print(f"Container Status: {status_count} records processed")

                print("Extracting location data...")
                location_count = extract_and_upsert_location(client, conn)
                print(f"Location: {location_count} records processed")

                print("Extracting order status data...")
                order_status_count = extract_and_upsert_order_status(client, conn)
                print(f"Order Status: {order_status_count} records processed")

                print("Extracting order header data...")
                order_hdr_count = extract_and_upsert_order_hdr(client, conn)
                print(f"Order Header: {order_hdr_count} records processed")

                print("Extracting order detail data...")
                order_dtl_count = extract_and_upsert_order_dtl(client, conn)
                print(f"Order Detail: {order_dtl_count} records processed")

            print("Extraction completed successfully!")
            print(
                {
                    "inventory_upserted": inv_count,
                    "container_upserted": cont_count,
                    "container_status_upserted": status_count,
                    "location_upserted": location_count,
                    "order_status_upserted": order_status_count,
                    "order_hdr_upserted": order_hdr_count,
                    "order_dtl_upserted": order_dtl_count,
                }
            )

    except Exception as e:
        print(f"Error during extraction: {e}")
        raise


if __name__ == "__main__":
    main()
