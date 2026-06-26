import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from eth_dhis2_tasks.utils import DHIS2Session

logger = logging.getLogger("airflow.task")

def parse_date(date_str):
    """Parses DHIS2 date strings into Postgres-friendly format."""
    if not date_str: 
        return None
    return date_str[:19].replace("T", " ")

def sync_completeness(**kwargs):
    """
    Syncs dataset completeness registrations from DHIS2.
    Targets OrgUnits based on the dc_downloadedat watermark.
    
    Note: Full Rebuilds are handled by clearing watermarks in a separate task.
    """
    # Configuration
    datasource = kwargs["DATA_SOURCE"].lower()
    staging = kwargs["STAGING_SCHEMA_NAME"]
    base_url = kwargs['URL'].rstrip('/')
    pg_conn_id = kwargs.get("POSTGRES_CONN_ID")
    
    # Hierarchy and Table definitions
    PARENT_ROOT_IDS = kwargs.get("ROOT_ORGUNIT_IDs", [])
    target_table = f"{datasource}_dataset_completeness"
    rel_table = f"{staging}.dataset_orgunits"
    period_table = f"{datasource}_periods"
    
    # 1. Logic for Date Filtering
    # Check if we should fetch all history or just recent data
    is_full_rebuild = kwargs.get("FULL_REBUILD", False)
    if isinstance(is_full_rebuild, str):
        is_full_rebuild = is_full_rebuild.lower() in ['true', '1', 't', 'y', 'yes']

    if is_full_rebuild:
        logger.info("FULL REBUILD MODE: Targetting all periods in the data values.")
        date_filter = ""
    else:
        logger.info("INCREMENTAL MODE: Applying 120-day lookback limit.")
        date_filter = "AND p.start_date >= (CURRENT_DATE - INTERVAL '120 days')"

    # 2. Watermark Filter (Always Active for Resume Support)
    # If a full rebuild was initiated, dc_downloadedat was set to NULL by the reset task.
    watermark_filter = "AND (wh_ou.dc_downloadedat < NOW() - INTERVAL '4 days' OR wh_ou.dc_downloadedat IS NULL)"

    pg = PostgresHook(postgres_conn_id=pg_conn_id)
    dhis = DHIS2Session(kwargs['URL'], kwargs["USERNAME"], kwargs["PASSWORD"])

    # 3. Build Hierarchy Filter Logic
    hierarchy_filter = ""
    if PARENT_ROOT_IDS:
        ids_str = ",".join([f"'{x}'" for x in PARENT_ROOT_IDS])
        hierarchy_filter = f"""
            AND EXISTS (
                SELECT 1 FROM unnest(string_to_array(wh_ou.oupath, '/')) as x 
                WHERE x IN ({ids_str})
            )
        """

    # 4. Build Work Query 
    work_query = f"""
        SELECT 
            rel.orgunit_id, 
            ARRAY_AGG(DISTINCT rel.dataset_id) as dataset_list,
            ARRAY_AGG(DISTINCT p.id) as period_list
        FROM {rel_table} rel
        INNER JOIN orgunits wh_ou 
            ON rel.orgunit_id = wh_ou.id 
        INNER JOIN datasets ds 
            ON rel.dataset_id = ds.id 
        INNER JOIN {period_table} p ON (
            CASE 
                WHEN ds.periodtype = 'Weekly' AND p.id ~ '^\d{{4}}W\d{{1,2}}$' THEN TRUE
                WHEN ds.periodtype = 'Monthly' AND p.id ~ '^\d{{6}}$' THEN TRUE
                WHEN ds.periodtype = 'Quarterly' AND p.id ~ '^\d{{4}}Q\d$' THEN TRUE
                WHEN ds.periodtype = 'SixMonthly' AND p.id ~ '^\d{{4}}S\d$' THEN TRUE
                WHEN ds.periodtype = 'Yearly' AND p.id ~ '^\d{{4}}$' THEN TRUE
                ELSE FALSE
            END
        )
        WHERE ds.datasource_id = %s
        {hierarchy_filter}
        {date_filter}
        {watermark_filter}
        GROUP BY rel.orgunit_id
    """

    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(work_query, (datasource,))
            work_list = cursor.fetchall()

            if not work_list:
                logger.info("No OrgUnits found requiring completeness sync.")
                return

            total_orgunits = len(work_list)
            total_records_updated = 0
            logger.info(f"Starting Completeness Sync for {total_orgunits} units.")

            # 5. Loop through OrgUnits and Fetch from DHIS2
            for idx, (ou_id, ds_list, p_list) in enumerate(work_list, 1):
                try:
                    ou_records_count = 0
                    
                    # Chunking Datasets (max 15 per request)
                    ds_chunk_size = 15 
                    for j in range(0, len(ds_list), ds_chunk_size):
                        ds_chunk = ds_list[j:j + ds_chunk_size]
                        
                        # Chunking Periods (max 80 per request)
                        p_chunk_size = 80
                        for k in range(0, len(p_list), p_chunk_size):
                            pe_chunk = p_list[k:k + p_chunk_size]
                            
                            params = {
                                "dataSet": ds_chunk,
                                "orgUnit": ou_id,
                                "period": pe_chunk,
                                "children": "false"
                            }

                            data = dhis.get("api/completeDataSetRegistrations.json", params=params)
                            registrations = data.get("completeDataSetRegistrations", [])

                            if registrations:
                                rows = []
                                for reg in registrations:
                                    rows.append((
                                        reg.get("dataSet"),
                                        reg.get("organisationUnit"),
                                        reg.get("period"),
                                        reg.get("attributeOptionCombo"),
                                        reg.get("storedBy"),
                                        parse_date(reg.get("date")),
                                        parse_date(reg.get("lastUpdated")),
                                        reg.get("completed", True)
                                    ))

                                upsert_sql = f"""
                                    INSERT INTO {target_table} (
                                        dataset_id, orgunit_id, period, attributeoptioncombo, 
                                        storedby, date_submitted, lastupdated, completed
                                    ) VALUES %s
                                    ON CONFLICT (dataset_id, orgunit_id, period, attributeoptioncombo) 
                                    DO UPDATE SET 
                                        date_submitted = EXCLUDED.date_submitted,
                                        lastupdated = EXCLUDED.lastupdated,
                                        completed = EXCLUDED.completed;
                                """
                                execute_values(cursor, upsert_sql, rows)
                                ou_records_count += len(rows)
                    
                    # 6. Update Watermark and COMMIT (The Checkpoint)
                    cursor.execute(
                        "UPDATE orgunits SET dc_downloadedat = CURRENT_DATE WHERE id = %s AND datasource_id = %s",
                        (ou_id, datasource)
                    )
                    
                    conn.commit()
                    total_records_updated += ou_records_count

                    if idx % 100 == 0 or idx == total_orgunits:
                        percent = (idx / total_orgunits) * 100
                        logger.info(f"Progress: {percent:.1f}% | OU {idx}/{total_orgunits} | Records: {ou_records_count}")

                except Exception as e:
                    conn.rollback()
                    logger.error(f"Sync failed for OU {ou_id}: {str(e)}")

    logger.info(f"Sync finished. Total registrations updated: {total_records_updated}")