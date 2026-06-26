from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger("airflow.task")

def prepare_full_rebuild(**kwargs):
    """
    Clears the target table and resets watermarks to force a full sync.
    Only executes if FULL_REBUILD is set to True.
    """
    is_full_rebuild = kwargs.get("FULL_REBUILD", False)
    target_table = kwargs.get("TARGET_TABLE")
    
    # Robust string check if passed via Airflow Params/Conf
    if isinstance(is_full_rebuild, str):
        is_full_rebuild = is_full_rebuild.lower() in ['true', '1', 't', 'y', 'yes']

    if not is_full_rebuild:
        logger.info("Incremental mode detected. Skipping Reset Task.")
        return

    datasource = kwargs.get("DATA_SOURCE").lower()
    pg_conn_id = kwargs.get("POSTGRES_CONN_ID")
    downloaded_at_field = kwargs.get("DOWNLOADED_AT_FIELD")
    pg = PostgresHook(postgres_conn_id=pg_conn_id)

    logger.info(f"FULL REBUILD INITIATED: Truncating {target_table} and resetting watermarks.")
    
    # Use a single transaction for the reset
    reset_sql = f"""
        TRUNCATE TABLE {target_table};
        UPDATE public.orgunits 
        SET {downloaded_at_field} = NULL 
        WHERE datasource_id = '{datasource}';
    """
    pg.run(reset_sql)
    logger.info("Reset complete. System is ready for a fresh sync.")