import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from eth_dhis2_tasks.utils import DHIS2Session

logger = logging.getLogger("airflow.task")

def sync_periods(**kwargs):
    """
    Finds periods present in datavalues but missing from the periods table,
    then downloads their metadata from DHIS2 in chunks of 30.
    """
    datasource = kwargs.get("DATA_SOURCE", "hmis").lower()
    dv_table = f"{datasource}_datavalues"
    period_table = f"{datasource}_periods"
    
    pg = PostgresHook(postgres_conn_id=kwargs.get("POSTGRES_CONN_ID"))
    dhis = DHIS2Session(kwargs['URL'], kwargs["USERNAME"], kwargs["PASSWORD"])

    # Identify missing periods using a LEFT JOIN
    # We look for periods in datavalues that don't exist in our metadata table
    missing_periods_query = f"""
        SELECT DISTINCT dv.period 
        FROM {dv_table} dv
        LEFT JOIN {period_table} p ON dv.period = p.id
        WHERE p.id IS NULL AND dv.period IS NOT NULL;
    """
    
    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(missing_periods_query)
            missing_periods = [row[0] for row in cursor.fetchall()]

    if not missing_periods:
        logger.info(f"No missing periods found for {datasource}. All metadata is up to date.")
        return

    logger.info(f"Found {len(missing_periods)} missing periods. Starting download in chunks of 30...")

    # Process in chunks of 30 to stay within DHIS2 URL limits
    chunk_size = 30
    for i in range(0, len(missing_periods), chunk_size):
        chunk = missing_periods[i:i + chunk_size]
        pe_param = ";".join(chunk)

        try:
            # Analytics endpoint trick to get startDate/endDate details
            params = {
                "dimension": f"pe:{pe_param}",
                "skipData": "true",
                "includeMetadataDetails": "true"
            }
            
            data = dhis.get("api/analytics", params=params)
            items = data.get("metaData", {}).get("items", {})
            rows_to_upsert = []

            for p_id in chunk:
                p_info = items.get(p_id)
                # Only save if DHIS2 successfully resolved the period dates
                if p_info and "startDate" in p_info:
                    rows_to_upsert.append((
                        p_id,
                        p_info["startDate"][:10], # Keep YYYY-MM-DD
                        p_info["endDate"][:10],
                        p_info.get("name")
                    ))

            # Upsert metadata into the specific datasource table
            if rows_to_upsert:
                upsert_sql = f"""
                    INSERT INTO {period_table} (id, start_date, end_date, period_name)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        start_date = EXCLUDED.start_date,
                        end_date = EXCLUDED.end_date,
                        period_name = EXCLUDED.period_name;
                """
                with pg.get_conn() as i_conn:
                    with i_conn.cursor() as i_cursor:
                        execute_values(i_cursor, upsert_sql, rows_to_upsert)
                        i_conn.commit()
                
                logger.info(f"Saved {len(rows_to_upsert)} periods for chunk {i//chunk_size + 1}")

        except Exception as e:
            logger.error(f"Failed to fetch metadata for chunk starting with {chunk[0]}: {e}")