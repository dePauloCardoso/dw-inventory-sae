## DW Inventory SAE - Daily Data Extraction

This project extracts today's data from Oracle WMS APIs and loads them into local Postgres tables in the `public` schema with idempotent upserts. The extraction pulls data from the current day (00:00:00 to 23:59:59) from all available endpoints.

### Setup

1. Install Python 3.12+ and Postgres.
2. Configure the application by editing `config.json`:

```json
{
    "wms": {
      "base_url": "your_url",
      "username": "your_username",
      "password": "your_password",
      "verify_ssl": true,
      "default_concurrency": 10,
      "default_timeout": 30.0,
      "default_retries": 3,
      "default_backoff_base": 0.5
    },
    "database": {
      "host": "your_host",
      "port": "your_port",
      "user": "your_user",
      "password": "your_password",
      "database": "dw_inventory_sae"
    }
}
```

**Important**: All database and WMS configuration values are required. The application will fail to start if any are missing.

3. Install dependencies (Poetry or pip):
   - Poetry: `poetry install`
   - Pip: `pip install -e .`

4. Create database tables (run once):
   ```bash
   psql -h $PGHOST -U $PGUSER -d $PGDATABASE -f sql/create_raw_container.sql
   ```

### Run

```
python main.py
```

This will:
- Extract today's data from WMS APIs (inventory, container, container_status)
- Filter data by create_ts for current day (00:00:00 to 23:59:59)
- Upsert data with proper type conversion and flattening
- Display progress and record counts

### Tables Created

- `public.raw_inventory` - Complete inventory data with flattened nested objects
- `public.raw_container` - Complete container data with all fields
- `public.raw_container_status` - Container status lookup table

### Features

- **Daily Data Extraction**: Pulls only today's data (00:00:00 to 23:59:59)
- **Date Filtering**: Uses create_ts__gte and create_ts__lt parameters for precise date range
- **Type Conversion**: Proper handling of dates, timestamps, numbers, and booleans
- **Nested Object Flattening**: Expands nested JSON objects into separate columns
- **Idempotent Upserts**: Safe to run multiple times without duplicates
- **JSON-based Configuration**: All settings in config.json file
- **Progress Tracking**: Real-time feedback during extraction

### Scheduling

Use Windows Task Scheduler or cron to run `python main.py` as needed for data refresh.


# dw-inventory-sae
# dw-inventory-sae
