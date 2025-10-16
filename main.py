from __future__ import annotations

from db import get_connection
from wms_client import WMSClient
from extractors.inventory import extract_and_upsert_inventory
from extractors.container import extract_and_upsert_container
from extractors.container_status import extract_and_upsert_container_status


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
            
            print("Extraction completed successfully!")
            print({
                "inventory_upserted": inv_count, 
                "container_upserted": cont_count,
                "container_status_upserted": status_count
            })
    
    except Exception as e:
        print(f"Error during extraction: {e}")
        raise


if __name__ == "__main__":
    main()


