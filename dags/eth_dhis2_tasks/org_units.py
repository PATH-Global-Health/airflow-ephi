import time
import logging
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from eth_dhis2_tasks.utils import DHIS2Session

logger = logging.getLogger(__name__)

def sync_org_units(**kwargs):
    # Parameters
    base_url = kwargs['URL'].rstrip('/')
    username = kwargs["USERNAME"]
    password = kwargs["PASSWORD"]
    pg_conn_id = kwargs.get("POSTGRES_CONN_ID")
    PAGE_SIZE = 100 
    staging = kwargs["STAGING_SCHEMA_NAME"]
    datasource = kwargs["DATA_SOURCE"]

    try:
        dhis = DHIS2Session(base_url, username, password)
    except Exception as e:
        logger.error(f"Critical: Failed to initialize DHIS2 Session: {e}")
        raise

    pg = PostgresHook(postgres_conn_id=pg_conn_id)
    
    # --- NEW: Fetch valid dataset IDs from the main datasets table ---
    # This ensures we don't insert assignments for datasets we aren't tracking
    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM datasets WHERE datasource_id = %s", (datasource,))
            valid_dataset_ids = {row[0] for row in cursor.fetchall()}
    
    logger.info(f"Filtering assignments against {len(valid_dataset_ids)} valid datasets in {datasource}")

    all_items = []
    page = 1
    endpoint = "api/organisationUnits.json"
    
    params = {
        "paging": "true", 
        "totalPages": "true", 
        "pageSize": str(PAGE_SIZE),
        "fields": "id,code,name,shortName,leaf,parent[id],path,level,openingDate,closedDate,lastUpdated,dataSets[id]"
    }

    while True:
        params["page"] = str(page)
        try:
            data = dhis.get(endpoint, params=params)
            items = data.get("organisationUnits", [])
            all_items.extend(items)
            pager = data.get("pager", {})
            page_count = pager.get("pageCount", 0)
            
            logger.info(f"Downloaded page {page} of {page_count} ({len(all_items)} units so far)")

            if page < page_count:
                page += 1
                time.sleep(0.05)
            else:
                break
        except Exception as e:
            logger.error(f"Error fetching Org Units on page {page}: {e}")
            raise

    ou_rows = []
    assignment_rows = []
    fetched_at = datetime.now()
    
    for i in all_items:
        ou_id = i.get("id")
        
        # Process Org Unit Row
        opening_date = i.get("openingDate")[:10] if i.get("openingDate") else None
        closed_date = i.get("closedDate")[:10] if i.get("closedDate") else None
        last_updated = i.get("lastUpdated")[:19].replace("T", " ") if i.get("lastUpdated") else None
        
        leaf_val = i.get("leaf")
        leaf_bool = leaf_val.lower() == "true" if isinstance(leaf_val, str) else bool(leaf_val)

        ou_rows.append((
            ou_id, i.get("code"), i.get("name"), i.get("shortName"), leaf_bool,
            (i.get("parent") or {}).get("id"), i.get("path"), i.get("level"),
            opening_date, closed_date, last_updated, fetched_at, datasource
        ))

        # Process Dataset Assignments with Filtering
        datasets = i.get("dataSets", [])
        for ds in datasets:
            ds_id = ds.get("id")
            # Only add assignment if the dataset exists in our local datasets table
            if ds_id and ds_id in valid_dataset_ids:
                assignment_rows.append((ds_id, ou_id))

    # Database Operations
    if ou_rows:
        try:
            with pg.get_conn() as conn:
                with conn.cursor() as cursor:
                    # Sync Org Units
                    logger.info(f"Truncating {staging}.orgunits and inserting {len(ou_rows)} rows...")
                    cursor.execute(f"TRUNCATE TABLE {staging}.orgunits")
                    ou_sql = f"""
                        INSERT INTO {staging}.orgunits (
                            id, code, name, shortname, leaf, parentid, 
                            path, level, openingdate, closeddate, lastupdated, _fetchedat, datasource_id
                        ) VALUES %s
                    """
                    execute_values(cursor, ou_sql, ou_rows)

                    # Sync Dataset Assignments
                    logger.info(f"Updating {staging}.dataset_orgunits with {len(assignment_rows)} filtered relations...")
                    cursor.execute(f"TRUNCATE TABLE {staging}.dataset_orgunits")
                    
                    rel_sql = f"""
                        INSERT INTO {staging}.dataset_orgunits (
                            dataset_id, orgunit_id
                        ) VALUES %s
                        ON CONFLICT (dataset_id, orgunit_id) DO NOTHING
                    """
                    if assignment_rows:
                        execute_values(cursor, rel_sql, assignment_rows)
                    
                    conn.commit()
            logger.info("Sync completed successfully with dataset filtering.")
        except Exception as e:
            logger.error(f"Database error: {e}")
            raise