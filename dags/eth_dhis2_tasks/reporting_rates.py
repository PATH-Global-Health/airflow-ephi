import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from eth_dhis2_tasks.utils import DHIS2Session, RateLimiter

logger = logging.getLogger("airflow.task")

_thread_local = threading.local()


def _get_dhis_session(base_url, username, password):
    if not hasattr(_thread_local, "dhis"):
        _thread_local.dhis = DHIS2Session(base_url, username, password)
    return _thread_local.dhis


def _process_batch(batch, base_url, username, password, pg, datasource, target_table, rate_limiter):
    """
    batch: list of (ou_id, ds_list, p_list)
    Fetches reporting rates for all OUs in the batch in combined analytics calls,
    then writes results and watermarks in a single DB transaction.
    """
    ou_ids = [row[0] for row in batch]
    try:
        all_datasets = list({ds for _, ds_list, _ in batch for ds in ds_list})
        all_periods  = list({p  for _, _,       p_list in batch for p  in p_list})

        dhis = _get_dhis_session(base_url, username, password)
        processed = {}

        ds_chunk_size = 10
        p_chunk_size  = 20

        for j in range(0, len(all_datasets), ds_chunk_size):
            ds_chunk = all_datasets[j:j + ds_chunk_size]
            metrics  = []
            for ds in ds_chunk:
                metrics.extend([
                    f"{ds}.EXPECTED_REPORTS",
                    f"{ds}.ACTUAL_REPORTS",
                    f"{ds}.ACTUAL_REPORTS_ON_TIME",
                ])

            for k in range(0, len(all_periods), p_chunk_size):
                pe_chunk = all_periods[k:k + p_chunk_size]
                params = {
                    "dimension": [
                        f"dx:{';'.join(metrics)}",
                        f"ou:{';'.join(ou_ids)}",
                        f"pe:{';'.join(pe_chunk)}",
                    ],
                    "skipMeta": "true",
                }

                rate_limiter.acquire()
                data = dhis.get("api/analytics.json", params=params)

                for dx_val, ou_val, pe_val, value in data.get("rows", []):
                    current_ds_id, metric_type = dx_val.split(".")
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

        with pg.get_conn() as conn:
            with conn.cursor() as cursor:
                if upsert_data:
                    upsert_sql = f"""
                        INSERT INTO {target_table} (
                            dataset_id, orgunit_id, period,
                            expected_reports, actual_reports, actual_reports_on_time
                        ) VALUES %s
                        ON CONFLICT (dataset_id, orgunit_id, period) DO UPDATE SET
                            expected_reports       = EXCLUDED.expected_reports,
                            actual_reports         = EXCLUDED.actual_reports,
                            actual_reports_on_time = EXCLUDED.actual_reports_on_time,
                            _ingestedat            = CURRENT_TIMESTAMP;
                    """
                    execute_values(cursor, upsert_sql, upsert_data)

                for ou_id in ou_ids:
                    cursor.execute(
                        "UPDATE orgunits SET rr_downloadedat = CURRENT_DATE "
                        "WHERE id = %s AND datasource_id = %s",
                        (ou_id, datasource),
                    )

                conn.commit()

        return len(upsert_data)

    except Exception as e:
        logger.error(f"Failed batch {ou_ids}: {e}")
        return 0


def sync_reporting_rates(**kwargs):
    """
    Syncs reporting rates (Expected, Actual, On-Time) from DHIS2 Analytics.
    OUs are batched together per analytics call and processed in parallel threads.
    """
    datasource = kwargs["DATA_SOURCE"].lower()
    staging    = kwargs["STAGING_SCHEMA_NAME"]
    base_url   = kwargs["URL"].rstrip("/")
    username   = kwargs["USERNAME"]
    password   = kwargs["PASSWORD"]
    pg_conn_id = kwargs.get("POSTGRES_CONN_ID")

    PARENT_ROOT_IDS = kwargs.get("ROOT_ORGUNIT_IDs", [])
    target_table    = f"{datasource}_reporting_rates"
    rel_table       = f"{staging}.dataset_orgunits"
    period_table    = f"{datasource}_periods"

    max_workers   = kwargs.get("MAX_WORKERS", 6)
    batch_size    = kwargs.get("BATCH_SIZE", 7)
    calls_per_sec = kwargs.get("CALLS_PER_SECOND", 5)

    is_full_rebuild = kwargs.get("FULL_REBUILD", False)
    if isinstance(is_full_rebuild, str):
        is_full_rebuild = is_full_rebuild.lower() in ["true", "1", "t", "y", "yes"]

    if is_full_rebuild:
        logger.info("FULL REBUILD MODE: Targeting all periods in the data values.")
        date_filter = ""
    else:
        logger.info("INCREMENTAL MODE: Applying 120-day lookback limit.")
        date_filter = "AND p.start_date >= (CURRENT_DATE - INTERVAL '120 days')"

    watermark_filter = "AND (wh_ou.rr_downloadedat < NOW() - INTERVAL '4 days' OR wh_ou.rr_downloadedat IS NULL)"

    pg           = PostgresHook(postgres_conn_id=pg_conn_id)
    rate_limiter = RateLimiter(calls_per_sec)

    hierarchy_filter = ""
    if PARENT_ROOT_IDS:
        ids_str = ",".join([f"'{x}'" for x in PARENT_ROOT_IDS])
        hierarchy_filter = f"""
            AND EXISTS (
                SELECT 1 FROM unnest(string_to_array(wh_ou.oupath, '/')) AS x
                WHERE x IN ({ids_str})
            )
        """

    work_query = f"""
        SELECT
            wh_ou.id                          AS orgunit_id,
            ARRAY_AGG(DISTINCT rel.dataset_id) AS dataset_list,
            ARRAY_AGG(DISTINCT p.id)           AS period_list
        FROM public.orgunits wh_ou
        JOIN {rel_table} rel ON wh_ou.id = rel.orgunit_id
        JOIN public.datasets ds ON rel.dataset_id = ds.id AND wh_ou.datasource_id = ds.datasource_id
        JOIN {period_table} p ON (
            CASE
                WHEN ds.periodtype = 'Weekly'     AND p.id ~ '^\d{{4}}W\d{{1,2}}$' THEN TRUE
                WHEN ds.periodtype = 'Monthly'    AND p.id ~ '^\d{{6}}$'            THEN TRUE
                WHEN ds.periodtype = 'Quarterly'  AND p.id ~ '^\d{{4}}Q\d$'         THEN TRUE
                WHEN ds.periodtype = 'SixMonthly' AND p.id ~ '^\d{{4}}S\d$'         THEN TRUE
                WHEN ds.periodtype = 'Yearly'     AND p.id ~ '^\d{{4}}$'            THEN TRUE
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
    batches        = [work_list[i:i + batch_size] for i in range(0, total_orgunits, batch_size)]
    total_batches  = len(batches)
    logger.info(f"Starting threaded sync for {total_orgunits} units across {total_batches} batches "
                f"({batch_size} OUs/batch) with {max_workers} workers at {calls_per_sec} calls/sec.")

    total_rows = 0
    completed  = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _process_batch,
                batch, base_url, username, password, pg, datasource, target_table, rate_limiter,
            ): idx
            for idx, batch in enumerate(batches, 1)
        }

        for future in as_completed(futures):
            total_rows += future.result()
            completed  += 1
            if completed % 10 == 0 or completed == total_batches:
                percent = (completed / total_batches) * 100
                logger.info(f"Progress: {percent:.1f}% | Batches {completed}/{total_batches} | "
                            f"Rows: {total_rows}")

    logger.info(f"Sync complete. {total_rows} reporting rate records updated.")
