import logging
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from eth_dhis2_tasks.utils import DHIS2Session

logger = logging.getLogger("airflow.task")

def sync_reporting_rates(**kwargs):
    """
    Syncs reporting rates (Expected, Actual, On-Time) from DHIS2 Analytics.
    Targets OrgUnits based on the rr_downloadedat watermark.
    """
    # Configuration
    datasource = kwargs["DATA_SOURCE"].lower()
    staging = kwargs["STAGING_SCHEMA_NAME"]
    base_url = kwargs['URL'].rstrip('/')
    pg_conn_id = kwargs.get("POSTGRES_CONN_ID")
    
    # Hierarchy and Table definitions
    PARENT_ROOT_IDS = kwargs.get("ROOT_ORGUNIT_IDs", [])
    target_table = f"{datasource}_reporting_rates"
    rel_table = f"{staging}.dataset_orgunits"
    period_table = f"{datasource}_periods"
    
    # 1. Determine Sync Mode for Date Filtering
    # We check the flag to decide IF we should limit the date range.
    is_full_rebuild = kwargs.get("FULL_REBUILD", False)
    if isinstance(is_full_rebuild, str):
        is_full_rebuild = is_full_rebuild.lower() in ['true', '1', 't', 'y', 'yes']

    if is_full_rebuild:
        logger.info("FULL REBUILD MODE: Targetting all periods in the data values.")
        date_filter = ""
    else:
        logger.info("INCREMENTAL MODE: Applying 120-day lookback limit.")
        date_filter = "AND p.start_date >= (CURRENT_DATE - INTERVAL '120 days')"

    # 2. Watermark Filter (Crucial for Resume)
    # This stays active in BOTH modes. If a full rebuild was triggered, 
    # the reset task set these to NULL. If it fails and restarts, 
    # the units already processed will have a CURRENT_DATE and be skipped.
    watermark_filter = "AND (wh_ou.rr_downloadedat < NOW() - INTERVAL '4 days' OR wh_ou.rr_downloadedat IS NULL)"

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
            wh_ou.id as orgunit_id, 
            ARRAY_AGG(DISTINCT rel.dataset_id) as dataset_list,
            ARRAY_AGG(DISTINCT p.id) as period_list
        FROM public.orgunits wh_ou
        JOIN {rel_table} rel ON wh_ou.id = rel.orgunit_id
        JOIN public.datasets ds ON rel.dataset_id = ds.id AND wh_ou.datasource_id = ds.datasource_id
        JOIN {period_table} p ON (
            CASE 
                WHEN ds.periodtype = 'Weekly' AND p.id ~ '^\d{{4}}W\d{{1,2}}$' THEN TRUE
                WHEN ds.periodtype = 'Monthly' AND p.id ~ '^\d{{6}}$' THEN TRUE
                WHEN ds.periodtype = 'Quarterly' AND p.id ~ '^\d{{4}}Q\d$' THEN TRUE
                WHEN ds.periodtype = 'SixMonthly' AND p.id ~ '^\d{{4}}S\d$' THEN TRUE
                WHEN ds.periodtype = 'Yearly' AND p.id ~ '^\d{{4}}$' THEN TRUE
                ELSE FALSE
            END
        )
        WHERE wh_ou.datasource_id = %s
        {date_filter}
        {hierarchy_filter}
        {watermark_filter}
        GROUP BY wh_ou.id
    """

    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(work_query, (datasource,))
            work_list = cursor.fetchall()

            if not work_list:
                logger.info("No OrgUnits found requiring reporting rate sync.")
                return

            total_orgunits = len(work_list)
            total_processed_rows = 0
            logger.info(f"Starting Sync for {total_orgunits} units.")

            # 5. Loop through OrgUnits and Fetch from Analytics API
            for idx, (ou_id, ds_list, p_list) in enumerate(work_list, 1):
                try:
                    ou_rows_count = 0
                    
                    # Chunking datasets to prevent 'URI Too Long' errors
                    ds_chunk_size = 10 
                    for j in range(0, len(ds_list), ds_chunk_size):
                        ds_chunk = ds_list[j:j + ds_chunk_size]
                        
                        metrics = []
                        for ds in ds_chunk:
                            metrics.extend([
                                f"{ds}.EXPECTED_REPORTS", 
                                f"{ds}.ACTUAL_REPORTS", 
                                f"{ds}.ACTUAL_REPORTS_ON_TIME"
                            ])

                        # Chunking periods
                        p_chunk_size = 20
                        for k in range(0, len(p_list), p_chunk_size):
                            pe_chunk = p_list[k:k + p_chunk_size]
                            
                            params = {
                                "dimension": [
                                    f"dx:{';'.join(metrics)}", 
                                    f"ou:{ou_id}", 
                                    f"pe:{';'.join(pe_chunk)}"
                                ],
                                "skipMeta": "true"
                            }

                            data = dhis.get("api/analytics.json", params=params)
                            rows = data.get("rows", [])
                            
                            if rows:
                                processed = {}
                                for dx_val, ou_val, pe_val, value in rows:
                                    current_ds_id, metric_type = dx_val.split('.')
                                    key = (current_ds_id, ou_val, pe_val)
                                    
                                    if key not in processed: 
                                        processed[key] = [0.0, 0.0, 0.0]
                                    
                                    val = float(value)
                                    if "EXPECTED" in metric_type: 
                                        processed[key][0] = val
                                    elif "ACTUAL_REPORTS_ON_TIME" in metric_type: 
                                        processed[key][2] = val
                                    elif "ACTUAL" in metric_type: 
                                        processed[key][1] = val

                                upsert_data = [(k[0], k[1], k[2], v[0], v[1], v[2]) for k, v in processed.items()]
                                
                                upsert_sql = f"""
                                    INSERT INTO {target_table} (
                                        dataset_id, orgunit_id, period, 
                                        expected_reports, actual_reports, actual_reports_on_time
                                    ) VALUES %s
                                    ON CONFLICT (dataset_id, orgunit_id, period) DO UPDATE SET 
                                        expected_reports = EXCLUDED.expected_reports,
                                        actual_reports = EXCLUDED.actual_reports,
                                        actual_reports_on_time = EXCLUDED.actual_reports_on_time,
                                        _ingestedat = CURRENT_TIMESTAMP;
                                """
                                execute_values(cursor, upsert_sql, upsert_data)
                                ou_rows_count += len(upsert_data)

                    # 6. Update Watermark & Commit
                    # This marks the unit as finished. Even if the task fails later, 
                    # this unit will NOT be in the work_list on the next retry.
                    cursor.execute(
                        "UPDATE orgunits SET rr_downloadedat = CURRENT_DATE WHERE id = %s AND datasource_id = %s",
                        (ou_id, datasource)
                    )
                    
                    conn.commit()
                    total_processed_rows += ou_rows_count

                    if idx % 100 == 0 or idx == total_orgunits:
                        percent = (idx / total_orgunits) * 100
                        logger.info(f"Progress: {percent:.1f}% | OU {idx}/{total_orgunits} | Rows: {ou_rows_count}")
                    
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Failed reporting rates sync for OU {ou_id}: {str(e)}")

    logger.info(f"Sync complete. {total_processed_rows} reporting rate records updated.")