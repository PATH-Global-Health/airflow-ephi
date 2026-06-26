import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from eth_dhis2_tasks.utils import DHIS2Session, to_ethiopian_string

logger = logging.getLogger("airflow.task")

def parse_value(raw_val):
    """Separates numeric values from string values for Postgres."""
    if raw_val is None or str(raw_val).strip() == "":
        return (None, None)
    s = str(raw_val).strip()
    try:
        return (None, float(s))
    except ValueError:
        return (s, None)

def sync_data_values(**kwargs):
    base_url = kwargs['URL'].rstrip('/')
    creds = (kwargs["USERNAME"], kwargs["PASSWORD"])
    pg_conn_id = kwargs.get("POSTGRES_CONN_ID")
    data_elements = kwargs["DATA_ELEMENTS"]
    default_start = kwargs["DEFAULT_START"]
    datasource = kwargs["DATA_SOURCE"] 
    target_table = f"{datasource}_datavalues"
    use_ethiopian = kwargs.get("USE_ETHIOPIAN_CALENDAR", False)
    download_all = kwargs.get("DOWNLOAD_ALL", False)

    pg = PostgresHook(postgres_conn_id=pg_conn_id)
    dhis = DHIS2Session(base_url, creds[0], creds[1])

    # If download_all is True, we force the watermark to be the default_start.
    # We also remove the '4 days' interval check to ensure everything is included.
    if download_all:
        logger.info(f"DOWNLOAD_ALL is TRUE. Ignoring watermarks and using {default_start} for all units.")
        get_leaves_sql = """
            SELECT id, %s::text 
            FROM orgunits
            WHERE leaf = TRUE 
            AND datasource_id = %s
        """
        query_params = (default_start, datasource)
    else:
        get_leaves_sql = """
            SELECT id, COALESCE(dv_downloadedat::text, %s) 
            FROM orgunits
            WHERE leaf = TRUE 
            AND datasource_id = %s
            AND (dv_downloadedat < NOW() - INTERVAL '4 days' OR dv_downloadedat IS NULL)
        """
        query_params = (default_start, datasource)
    
    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(get_leaves_sql, query_params)
            leaves = cursor.fetchall()
            
            if not leaves:
                logger.info("No OrgUnits found for sync.")
                return
            
            total_leaves = len(leaves)
            total_rows_synced = 0
            logger.info(f"Starting Individual Sync for {total_leaves} units into {target_table}.")

            for count, (ou_id, last_downloaded) in enumerate(leaves, 1):
                try:
                    # Determine start date based on calendar
                    # slicing [:10] to ensure we only get 'YYYY-MM-DD'
                    last_down = to_ethiopian_string(last_downloaded[:10]) if use_ethiopian else last_downloaded[:10]
                    
                    endpoint = "api/dataValueSets.json"
                    params = {
                        "orgUnit": ou_id,
                        "dataElement": ",".join(data_elements),
                        "lastUpdated": last_down
                    }
                    
                    if count % 100 == 0 or count == 1 or count == total_leaves:
                        percent = (count / total_leaves) * 100
                        logger.info(f"--- Progress: {percent:.1f}% ({count}/{total_leaves}) ---")
                        logger.info(f"OU: {ou_id} | Syncing from: {last_down}")

                    data = dhis.get(endpoint, params=params)
                    items = data.get("dataValues", [])

                    if items:
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
                                dv.get("deleted")                
                            ))
                        
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
                                lastupdated = EXCLUDED.lastupdated,
                                deleted = EXCLUDED.deleted;
                        """
                        execute_values(cursor, upsert_sql, rows)
                        total_rows_synced += len(rows)
                    
                    cursor.execute(
                        "UPDATE orgunits SET dv_downloadedat = CURRENT_DATE WHERE id = %s AND datasource_id = %s",
                        (ou_id, datasource)
                    )
                    conn.commit()

                except Exception as e:
                    conn.rollback()
                    logger.error(f"Failed OrgUnit {ou_id}: {str(e)}")

    logger.info(f"Sync Finished. Total Values Upserted: {total_rows_synced}")