create schema if not exists public;

-- Container table
create table if not exists public.raw_container (
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
    curr_location_id_id bigint,
    curr_location_id_key text,
    curr_location_id_url text,
    prev_location_id_id bigint,
    prev_location_id_key text,
    prev_location_id_url text,
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
    pallet_position text,

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

    -- location id (scalar)
    location_id         bigint,

    -- nested: container_id
    container_id_id     bigint,
    container_id_key    text,
    container_id_url    text,

    -- inventory attributes
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
    uom_id_url          text,

    -- nested: location_id
    location_id_id      bigint,
    location_id_key     text,
    location_id_url     text,

    -- container id (scalar)
    container_id        bigint
);

-- Container Status table
create table if not exists public.raw_container_status (
    -- core fields
    id                  integer primary key,
    description         text not null
);

-- Order Detail table
create table if not exists public.raw_order_dtl (
    -- core identifiers
    id                  bigint primary key,
    url                 text,

    -- audit
    create_user         text,
    create_ts           timestamp with time zone,
    mod_user            text,
    mod_ts              timestamp with time zone,

    -- nested: order_id
    order_id_id         bigint,
    order_id_key        text,
    order_id_url        text,

    seq_nbr             integer,

    -- nested: item_id
    item_id_id          bigint,
    item_id_key         text,
    item_id_url         text,

    ord_qty             numeric,
    orig_ord_qty        numeric,
    alloc_qty           numeric,
    req_cntr_nbr        text,
    po_nbr              text,
    shipment_nbr        text,
    dest_facility_attr_a text,
    dest_facility_attr_b text,
    dest_facility_attr_c text,
    ref_nbr_1           text,
    vas_activity_code   text,
    cost                numeric,
    sale_price          numeric,
    host_ob_lpn_nbr     text,
    spl_instr           text,
    batch_number_id     bigint,
    voucher_nbr         text,
    voucher_amount      numeric,
    voucher_exp_date    timestamp with time zone,
    req_pallet_nbr      text,
    lock_code           text,
    serial_nbr          text,
    voucher_print_count integer,
    ship_request_line   text,
    unit_declared_value numeric,
    externally_planned_load_nbr text,

    -- nested: invn_attr_id
    invn_attr_id_id     bigint,
    invn_attr_id_key    text,
    invn_attr_id_url    text,

    internal_text_field_1 text,
    orig_item_code      text,
    cust_field_1        text,
    cust_field_2        text,
    cust_field_3        text,
    cust_field_4        text,
    cust_field_5        text,
    cust_date_1         timestamp with time zone,
    cust_date_2         timestamp with time zone,
    cust_date_3         timestamp with time zone,
    cust_date_4         timestamp with time zone,
    cust_date_5         timestamp with time zone,
    cust_number_1       numeric,
    cust_number_2       numeric,
    cust_number_3       numeric,
    cust_number_4       numeric,
    cust_number_5       numeric,
    cust_decimal_1      numeric,
    cust_decimal_2      numeric,
    cust_decimal_3      numeric,
    cust_decimal_4      numeric,
    cust_decimal_5      numeric,
    cust_short_text_1   text,
    cust_short_text_2   text,
    cust_short_text_3   text,
    cust_short_text_4   text,
    cust_short_text_5   text,
    cust_short_text_6   text,
    cust_short_text_7   text,
    cust_short_text_8   text,
    cust_short_text_9   text,
    cust_short_text_10  text,
    cust_short_text_11  text,
    cust_short_text_12  text,
    cust_long_text_1    text,
    cust_long_text_2    text,
    cust_long_text_3    text,
    order_instructions_set text,
    erp_source_line_ref text,
    erp_source_shipment_ref text,
    erp_fulfillment_line_ref text,
    min_shipping_tolerance_percentage numeric,
    max_shipping_tolerance_percentage numeric,
    status_id           integer,
    order_dtl_original_seq_nbr text,

    -- nested: uom_id
    uom_id_id           bigint,
    uom_id_key          text,
    uom_id_url          text,

    -- nested: ordered_uom_id
    ordered_uom_id_id   bigint,
    ordered_uom_id_key  text,
    ordered_uom_id_url  text,

    ordered_uom_qty     numeric,
    required_serial_nbr_set text,
    ob_lpn_type_id      bigint,
    planned_parcel_shipment_nbr text,
    orig_order_ref_id   bigint
);

-- Order Header table
create table if not exists public.raw_order_hdr (
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

    order_nbr           text,

    -- nested: order_type_id
    order_type_id_id    bigint,
    order_type_id_key   text,
    order_type_id_url   text,

    status_id           integer,
    ord_date            date,
    exp_date            date,
    req_ship_date       date,
    dest_facility_id    bigint,
    shipto_facility_id  bigint,

    -- customer information
    cust_name           text,
    cust_addr           text,
    cust_addr2          text,
    cust_addr3          text,
    cust_city           text,
    cust_state          text,
    cust_zip            text,
    cust_country        text,
    cust_phone_nbr      text,
    cust_email          text,
    cust_nbr            text,

    -- ship to information
    shipto_name         text,
    shipto_addr         text,
    shipto_addr2        text,
    shipto_addr3        text,
    shipto_city         text,
    shipto_state        text,
    shipto_zip          text,
    shipto_country      text,
    shipto_phone_nbr    text,
    shipto_email        text,

    ref_nbr             text,
    stage_location_id   bigint,
    ship_via_ref_code   text,
    route_nbr           text,
    external_route      text,

    -- nested: destination_company_id
    destination_company_id_id    bigint,
    destination_company_id_key   text,
    destination_company_id_url   text,

    ship_via_id         bigint,
    priority            integer,
    host_allocation_nbr text,
    sales_order_nbr     text,
    sales_channel       text,
    customer_po_nbr     text,
    carrier_account_nbr text,
    payment_method_id   bigint,
    dest_dept_nbr       text,
    start_ship_date     date,
    stop_ship_date      date,
    vas_group_code      text,
    spl_instr           text,
    currency_code       text,
    record_origin_code  text,
    cust_contact        text,
    shipto_contact      text,
    ob_lpn_type         text,
    ob_lpn_type_id      bigint,
    total_orig_ord_qty  numeric,
    orig_sku_count      integer,
    orig_sale_price     numeric,
    gift_msg            text,
    sched_ship_date     date,
    customer_po_type    text,
    customer_vendor_code text,
    externally_planned_load_flg boolean,
    work_order_kit_id   bigint,
    order_nbr_to_replace text,
    stop_ship_flg       boolean,
    lpn_type_class      text,
    billto_carrier_account_nbr text,
    duties_carrier_account_nbr text,
    duties_payment_method_id bigint,
    customs_broker_contact_id bigint,
    order_shipped_ts    timestamp with time zone,

    -- custom fields
    cust_field_1        text,
    cust_field_2        text,
    cust_field_3        text,
    cust_field_4        text,
    cust_field_5        text,
    cust_date_1         date,
    cust_date_2         date,
    cust_date_3         date,
    cust_date_4         date,
    cust_date_5         date,
    cust_number_1       numeric,
    cust_number_2       numeric,
    cust_number_3       numeric,
    cust_number_4       numeric,
    cust_number_5       numeric,
    cust_decimal_1      numeric,
    cust_decimal_2      numeric,
    cust_decimal_3      numeric,
    cust_decimal_4      numeric,
    cust_decimal_5      numeric,
    cust_short_text_1   text,
    cust_short_text_2   text,
    cust_short_text_3   text,
    cust_short_text_4   text,
    cust_short_text_5   text,
    cust_short_text_6   text,
    cust_short_text_7   text,
    cust_short_text_8   text,
    cust_short_text_9   text,
    cust_short_text_10  text,
    cust_short_text_11  text,
    cust_short_text_12  text,
    cust_long_text_1    text,
    cust_long_text_2    text,
    cust_long_text_3    text,
    order_instructions_set text,

    -- nested: order_dtl_set
    order_dtl_set_result_count integer,
    order_dtl_set_url   text,

    order_lock_set      text,
    tms_parcel_shipment_nbr text,
    erp_source_hdr_ref  text,
    erp_source_system_ref text,
    tms_order_hdr_ref   text,
    group_ref           text
);

-- Order Status table
create table if not exists public.raw_order_status (
    -- core fields
    id                  integer primary key,
    description         text not null
);

-- Location table
create table if not exists public.raw_location (
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

    -- nested: dedicated_company_id
    dedicated_company_id_id     bigint,
    dedicated_company_id_key     text,
    dedicated_company_id_url    text,

    -- location attributes
    area                text,
    aisle               text,
    bay                 text,
    level               text,
    position             text,
    bin                 text,

    -- nested: type_id
    type_id_id          bigint,
    type_id_key         text,
    type_id_url         text,

    allow_multi_sku     boolean,
    barcode             text,

    -- nested: destination_company_id
    destination_company_id_id    bigint,

    -- dimensions and capacity
    length              numeric,
    width               numeric,
    height              numeric,
    max_units           numeric,
    max_lpns            bigint,

    -- counting and locking
    to_be_counted_flg   boolean,
    to_be_counted_ts    timestamp with time zone,
    lock_code_id        bigint,
    lock_for_putaway_flg boolean,
    pick_seq            text,
    last_count_ts       timestamp with time zone,
    last_count_user     text,
    locn_size_type_id   bigint,
    min_units           numeric,
    allow_reserve_partial_pick_flg boolean,
    alloc_zone          text,
    locn_str            text,
    putaway_seq         text,

    -- nested: replenishment_zone_id
    replenishment_zone_id_id    bigint,
    replenishment_zone_id_key    text,
    replenishment_zone_id_url    text,

    -- volume and weight constraints
    min_volume           numeric,
    max_volume           numeric,
    restrict_batch_nbr_flg boolean,

    -- nested: item_assignment_type_id
    item_assignment_type_id_id     bigint,
    item_assignment_type_id_key     text,
    item_assignment_type_id_url     text,

    -- nested: item_id
    item_id_id          bigint,
    item_id_key         text,
    item_id_url         text,

    mhe_system_id       bigint,
    pick_zone           text,
    divert_lane         text,
    task_zone_id        bigint,
    in_transit_units    numeric,
    restrict_invn_attr_flg boolean,
    assembly_flg        boolean,
    billing_location_type text,

    -- custom fields
    cust_field_1        text,
    cust_field_2        text,
    cust_field_3        text,
    cust_field_4        text,
    cust_field_5        text,

    -- weight constraints
    min_weight          numeric,
    max_weight          numeric,

    -- nested: cc_threshold_uom_id
    cc_threshold_uom_id_id      bigint,
    cc_threshold_uom_id_key     text,
    cc_threshold_uom_id_url    text,
    cc_threshold_value         numeric,

    -- coordinates
    x_coordinate        numeric,
    y_coordinate        numeric,
    z_coordinate        numeric,
    lock_applied_ts     timestamp with time zone,
    ignore_attr_values_for_restrict_invn_attr text,
    ranking             text
);
