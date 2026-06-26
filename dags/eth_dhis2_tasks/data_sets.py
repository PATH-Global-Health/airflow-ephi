import time
import logging
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from eth_dhis2_tasks.utils import DHIS2Session

# Get Airflow's logger
logger = logging.getLogger(__name__)

def sync_datasets(**kwargs):
    # Setup parameters
    url = kwargs["URL"].rstrip("/")
    username = kwargs["USERNAME"]
    password = kwargs["PASSWORD"]
    data_elements_list = kwargs["DATA_ELEMENTS"]
    postgres_conn_id = kwargs.get("POSTGRES_CONN_ID")
    datasource = kwargs["DATA_SOURCE"]
    staging = kwargs["STAGING_SCHEMA_NAME"]
    
    pg = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    # Initialize API Session
    try:
        dhis = DHIS2Session(url, username, password)
    except Exception as e:
        logger.error(f"Failed to initialize DHIS2 Session: {e}")
        raise
    
    # Discovery: Identify unique Dataset IDs and Mapping Relationships
    dataset_ids = set()
    raw_mapping = set() # Using a set to avoid duplicate relationships in memory
    
    logger.info(f"Scanning {len(data_elements_list)} data elements for dataset associations...")
    
    for de_id in data_elements_list:
        try:
            # Fetch dataSetElements to bridge Data Elements to Datasets
            data = dhis.get(f"api/dataElements/{de_id}.json", params={"fields": "dataSetElements[dataSet[id]]"})
            
            for dse in data.get("dataSetElements", []):
                ds = dse.get("dataSet", {})
                ds_id = ds.get("id")
                if ds_id:
                    dataset_ids.add(ds_id)
                    raw_mapping.add((ds_id, de_id))
        except Exception as e:
            logger.warning(f"Could not resolve datasets for Data Element {de_id}: {e}")

    if not dataset_ids:
        logger.info("No associated datasets found. Sync complete.")
        return

    # --- PART 1: Sync Mapping Bridge Table (Staging) ---
    if raw_mapping:
        logger.info(f"Syncing {len(raw_mapping)} mapping rows to {staging}.dataset_dataelements...")
        
        # Prepare tuples with the timestamp to match the 3-column SQL target
        now = datetime.now()
        mapping_values = [(row[0], row[1], now) for row in raw_mapping]

        mapping_sql = f"""
            INSERT INTO {staging}.dataset_dataelements (dataset_id, dataelement_id, _fetchedat)
            VALUES %s
            ON CONFLICT (dataset_id, dataelement_id) 
            DO UPDATE SET _fetchedat = EXCLUDED._fetchedat;
        """
        
        with pg.get_conn() as conn:
            with conn.cursor() as cursor:
                # execute_values now receives 3 values per row to match 3 target columns
                execute_values(cursor, mapping_sql, mapping_values)
                conn.commit()

    # --- PART 2: Sync Dataset Metadata (Warehouse) ---
    fields = "id,code,name,shortName,displayName,description,periodType,categoryCombo[id,name],created,lastUpdated,href"
    processed_count = 0
    error_count = 0

    logger.info(f"Syncing metadata for {len(dataset_ids)} discovered datasets into warehouse...")

    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            for ds_id in dataset_ids:
                try:
                    i = dhis.get(f"api/dataSets/{ds_id}.json", params={"fields": fields})

                    record = {
                        "id": i.get("id"),
                        "code": i.get("code"),
                        "name": i.get("name"),
                        "shortname": i.get("shortName"),
                        "displayname": i.get("displayName"),
                        "description": i.get("description"),
                        "periodtype": i.get("periodType"),
                        "categorycombo_id": (i.get("categoryCombo") or {}).get("id"),
                        "categorycombo_name": (i.get("categoryCombo") or {}).get("name"),
                        "created": i.get("created")[:23] if i.get("created") else None,
                        "lastupdated": i.get("lastUpdated")[:23] if i.get("lastUpdated") else None,
                        "href": i.get("href"),
                        "_fetchedat": datetime.now(),
                        "datasource_id": datasource
                    }

                    cols = list(record.keys())
                    vals = [record[k] for k in cols]
                    placeholders = ", ".join(["%s"] * len(vals))
                    updates = ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in cols if c not in ["id", "datasource_id", "created"]])

                    sql = f"""
                        INSERT INTO datasets ({', '.join([f'"{c}"' for c in cols])}) 
                        VALUES ({placeholders}) 
                        ON CONFLICT (id, datasource_id) DO UPDATE SET {updates}
                        WHERE EXCLUDED."lastupdated" > datasets."lastupdated" 
                           OR datasets."lastupdated" IS NULL
                    """
                    
                    cursor.execute(sql, vals)
                    conn.commit()
                    processed_count += 1
                    
                    time.sleep(0.05)

                except Exception as e:
                    error_count += 1
                    conn.rollback()
                    logger.error(f"FAILED to sync dataSet {ds_id}: {str(e)}")

    logger.info(f"Datasets sync finished. Processed: {processed_count}, Errors: {error_count}")
    
    if error_count > (len(dataset_ids) * 0.5):
        raise Exception("Task failed: More than 50% of discovered datasets failed to sync.")