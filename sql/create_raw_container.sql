create schema if not exists public;

-- Container table
create table if not exists public.raw_container (
    -- ingestion metadata
    _ingested_at        timestamp without time zone default now() not null,
    _source             varchar default 'wms' not null,

    -- core identifiers
    id                  bigint primary key,
    url                 text,

    -- audit
    create_user         text,
    create_ts           timestamp with time zone,
    mod_user            text,
    mod_ts              timestamp with time zone,

    -- nested: facility_id
    facility_id_id      bigint,
    facility_id_key     text,
    facility_id_url     text,

    -- nested: company_id
    company_id_id       bigint,
    company_id_key      text,
    company_id_url      text,

    -- main attributes
    container_nbr       text,
    type                text,
    status_id           integer,
    vas_status_id       integer,
    curr_location_id    bigint,
    prev_location_id    bigint,
    priority_date       timestamp with time zone,
    pallet_id           bigint,

    -- nested: rcvd_shipment_id
    rcvd_shipment_id_id     bigint,
    rcvd_shipment_id_key    text,
    rcvd_shipment_id_url    text,

    rcvd_ts             timestamp with time zone,
    rcvd_user           text,

    weight              numeric,
    volume              numeric,
    pick_user           text,
    pack_user           text,

    -- nested: putawaytype_id
    putawaytype_id_id   bigint,
    putawaytype_id_key  text,
    putawaytype_id_url  text,

    ref_iblpn_nbr       text,
    ref_shipment_nbr    text,
    ref_po_nbr          text,
    ref_oblpn_nbr       text,

    first_putaway_ts    timestamp with time zone,
    parcel_batch_flg    boolean,
    lpn_type_id         bigint,
    cart_posn_nbr       integer,
    audit_status_id     integer,
    qc_status_id        integer,
    asset_id            bigint,
    asset_seal_nbr      text,
    price_labels_printed boolean,
    comments            text,
    actual_weight_flg   boolean,

    length              numeric,
    width               numeric,
    height              numeric,

    rcvd_trailer_nbr    text,
    orig_container_nbr  text,

    inventory_lock_set  text,
    nbr_files           integer,
    cust_field_1        text,
    cust_field_2        text,
    cust_field_3        text,
    cust_field_4        text,
    cust_field_5        text,
    cart_nbr            text
);

-- Inventory table
create table if not exists public.raw_inventory (
    -- ingestion metadata
    _ingested_at        timestamp without time zone default now() not null,
    _source             varchar default 'wms' not null,

    -- core identifiers
    id                  bigint primary key,
    url                 text,

    -- audit
    create_user         text,
    create_ts           timestamp with time zone,
    mod_user            text,
    mod_ts              timestamp with time zone,

    -- nested: facility_id
    facility_id_id      bigint,
    facility_id_key     text,
    facility_id_url     text,

    -- nested: item_id
    item_id_id          bigint,
    item_id_key         text,
    item_id_url         text,

    location_id         bigint,

    -- nested: container_id
    container_id_id     bigint,
    container_id_key    text,
    container_id_url    text,

    priority_date       date,
    curr_qty            numeric,
    orig_qty            numeric,
    pack_qty            numeric,
    case_qty            numeric,
    status_id           integer,

    manufacture_date    date,
    expiry_date         date,
    batch_number_id     bigint,

    -- nested: invn_attr_id
    invn_attr_id_id     bigint,
    invn_attr_id_key    text,
    invn_attr_id_url    text,

    serial_nbr_set      text,

    -- nested: uom_id
    uom_id_id           bigint,
    uom_id_key          text,
    uom_id_url          text
);

-- Container Status table
create table if not exists public.raw_container_status (
    -- ingestion metadata
    _ingested_at        timestamp without time zone default now() not null,
    _source             varchar default 'wms' not null,

    -- core fields
    id                  integer primary key,
    description         text not null
);


