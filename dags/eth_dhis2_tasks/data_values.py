import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from eth_dhis2_tasks.utils import DHIS2Session, RateLimiter, to_ethiopian_string

logger = logging.getLogger("airflow.task")

_thread_local = threading.local()


def _get_dhis_session(base_url, username, password):
    if not hasattr(_thread_local, "dhis"):
        _thread_local.dhis = DHIS2Session(base_url, username, password)
    return _thread_local.dhis


def parse_value(raw_val):
    """Separates numeric values from string values for Postgres."""
    if raw_val is None or str(raw_val).strip() == "":
        return (None, None)
    s = str(raw_val).strip()
    try:
        return (None, float(s))
    except ValueError:
        return (s, None)


def _process_ou(ou_id, last_downloaded, base_url, username, password, pg,
                data_elements, datasource, target_table, use_ethiopian, rate_limiter):
    try:
        last_down = to_ethiopian_string(last_downloaded[:10]) if use_ethiopian else last_downloaded[:10]

        dhis = _get_dhis_session(base_url, username, password)
        params = {
            "orgUnit": ou_id,
            "dataElement": ",".join(data_elements),
            "lastUpdated": last_down,
        }

        rate_limiter.acquire()
        data = dhis.get("api/dataValueSets.json", params=params)
        items = data.get("dataValues", [])

        rows = []
        for dv in items:
            val_str, val_num = parse_value(dv.get("value"))
            rows.append((
                dv.get("orgUnit"),
                dv.get("dataElement"),
                dv.get("period"),
                dv.get("categoryOptionCombo"),
                dv.get("attributeOptionCombo"),
                val_str,
                val_num,
                dv.get("comment"),
                dv.get("storedBy"),
                (dv.get("created") or "")[:19].replace("T", " "),
                (dv.get("lastUpdated") or "")[:19].replace("T", " "),
                dv.get("followUp"),
                dv.get("deleted"),
            ))

        with pg.get_conn() as conn:
            with conn.cursor() as cursor:
                if rows:
                    upsert_sql = f"""
                        INSERT INTO {target_table} (
                            orgunit, dataelement, period, categoryoptioncombo,
                            attributeoptioncombo, value_string, value_double,
                            comment, storedby, created, lastupdated, followup, deleted
                        ) VALUES %s
                        ON CONFLICT (orgunit, dataelement, period, categoryoptioncombo, attributeoptioncombo)
                        DO UPDATE SET
                            value_string = EXCLUDED.value_string,
                            value_double = EXCLUDED.value_double,
                            lastupdated  = EXCLUDED.lastupdated,
                            deleted      = EXCLUDED.deleted;
                    """
                    execute_values(cursor, upsert_sql, rows)

                cursor.execute(
                    "UPDATE orgunits SET dv_downloadedat = CURRENT_DATE WHERE id = %s AND datasource_id = %s",
                    (ou_id, datasource),
                )
                conn.commit()

        return len(rows)

    except Exception as e:
        logger.error(f"Failed OrgUnit {ou_id}: {e}")
        return 0


def sync_data_values(**kwargs):
    base_url       = kwargs["URL"].rstrip("/")
    username       = kwargs["USERNAME"]
    password       = kwargs["PASSWORD"]
    pg_conn_id     = kwargs.get("POSTGRES_CONN_ID")
    data_elements  = kwargs["DATA_ELEMENTS"]
    default_start  = kwargs["DEFAULT_START"]
    datasource     = kwargs["DATA_SOURCE"]
    target_table   = f"{datasource}_datavalues"
    use_ethiopian  = kwargs.get("USE_ETHIOPIAN_CALENDAR", False)
    download_all   = kwargs.get("DOWNLOAD_ALL", False)
    max_workers    = kwargs.get("MAX_WORKERS", 6)
    calls_per_sec  = kwargs.get("CALLS_PER_SECOND", 5)

    pg           = PostgresHook(postgres_conn_id=pg_conn_id)
    rate_limiter = RateLimiter(calls_per_sec)

    if download_all:
        logger.info(f"DOWNLOAD_ALL is TRUE. Ignoring watermarks and using {default_start} for all units.")
        get_leaves_sql = """
            SELECT id, %s::text
            FROM orgunits
            WHERE leaf = TRUE
            AND datasource_id = %s
        """
    else:
        get_leaves_sql = """
            SELECT id, COALESCE(dv_downloadedat::text, %s)
            FROM orgunits
            WHERE leaf = TRUE
            AND datasource_id = %s
            AND (dv_downloadedat < NOW() - INTERVAL '4 days' OR dv_downloadedat IS NULL)
        """

    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(get_leaves_sql, (default_start, datasource))
            leaves = cursor.fetchall()

    if not leaves:
        logger.info("No OrgUnits found for sync.")
        return

    total_leaves = len(leaves)
    logger.info(f"Starting threaded sync for {total_leaves} units into {target_table} "
                f"with {max_workers} workers at {calls_per_sec} calls/sec.")

    total_rows_synced = 0
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _process_ou,
                ou_id, last_downloaded, base_url, username, password, pg,
                data_elements, datasource, target_table, use_ethiopian, rate_limiter,
            ): ou_id
            for ou_id, last_downloaded in leaves
        }

        for future in as_completed(futures):
            total_rows_synced += future.result()
            completed += 1
            if completed % 100 == 0 or completed == total_leaves:
                percent = (completed / total_leaves) * 100
                logger.info(f"Progress: {percent:.1f}% ({completed}/{total_leaves}) | "
                            f"Rows synced: {total_rows_synced}")

    logger.info(f"Sync Finished. Total Values Upserted: {total_rows_synced}")
