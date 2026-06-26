import time
import logging
from datetime import datetime
from collections import OrderedDict
from airflow.providers.postgres.hooks.postgres import PostgresHook
from eth_dhis2_tasks.utils import DHIS2Session

# Get Airflow's logger for better visibility in the UI logs
logger = logging.getLogger(__name__)

def collect_category_combos(item: dict):
    """
    Combines root and dataset categoryCombos, de-duplicated.
    """
    ordered = OrderedDict()
    root = (item.get("categoryCombo") or {})
    root_id, root_name = root.get("id"), root.get("name")
    if root_id:
        ordered.setdefault(root_id, root_name or "")

    for dse in (item.get("dataSetElements") or []):
        cc = (dse.get("categoryCombo") or {})
        cid, cname = cc.get("id"), cc.get("name")
        if cid:
            if cid not in ordered:
                ordered[cid] = cname or ""
            elif not ordered[cid] and cname:
                ordered[cid] = cname
    return ",".join(ordered.keys()), ",".join(v for v in ordered.values() if v)

def sync_data_elements(**kwargs):
    # Parameters
    url = kwargs["URL"].rstrip("/")
    username = kwargs["USERNAME"]
    password = kwargs["PASSWORD"]
    data_elements_list = kwargs["DATA_ELEMENTS"]
    postgres_conn_id = kwargs.get("POSTGRES_CONN_ID")
    datasource = kwargs["DATA_SOURCE"]

    pg = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    # Setup API Session (Handles authentication internally)
    try:
        dhis = DHIS2Session(url, username, password)
    except Exception as e:
        logger.error(f"Critical: Failed to initialize DHIS2 Session: {e}")
        raise

    fields = (
        "id,code,name,shortName,displayName,formName,description,"
        "valueType,domainType,aggregationType,zeroIsSignificant,"
        "categoryCombo[id,name],optionSet[id,name],"
        "dataSetElements[categoryCombo[id,name],dataSet[id,name]],"
        "created,lastUpdated,href"
    )

    processed_count = 0
    error_count = 0

    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            for de_id in data_elements_list:
                try:
                    # Fetch from DHIS2 
                    # Note: dhis.get already handles .raise_for_status() and .json()
                    i = dhis.get(f"api/dataElements/{de_id}.json", params={"fields": fields})
                    
                    if not i or "id" not in i:
                        logger.warning(f"Data element {de_id} returned empty data.")
                        continue

                    cat_ids, cat_names = collect_category_combos(i)
                    
                    # Prepare Record
                    record = {
                        "id": i.get("id"),
                        "code": i.get("code"),
                        "name": i.get("name"),
                        "shortname": i.get("shortName"),
                        "displayname": i.get("displayName"),
                        "formname": i.get("formName"),
                        "description": i.get("description"),
                        "valuetype": i.get("valueType"),
                        "domaintype": i.get("domainType"),
                        "aggregationtype": i.get("aggregationType"),
                        "zeroissignificant": str(i.get("zeroIsSignificant")).lower() == "true",
                        "categorycombo_id": cat_ids,
                        "categorycombo_name": cat_names,
                        "optionset_id": (i.get("optionSet") or {}).get("id"),
                        "optionset_name": (i.get("optionSet") or {}).get("name"),
                        "created": i.get("created")[:23] if i.get("created") else None,
                        "lastupdated": i.get("lastUpdated")[:23] if i.get("lastUpdated") else None,
                        "href": i.get("href"),
                        "_fetchedat": datetime.now(),
                        "datasource_id": datasource
                    }

                    # Upsert Logic
                    cols = list(record.keys())
                    vals = [record[k] for k in cols]
                    placeholders = ", ".join(["%s"] * len(vals))
                    updates = ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in cols if c not in ["id", "datasource_id", "created"]])

                    sql = f"""
                        INSERT INTO dataelements ({', '.join([f'"{c}"' for c in cols])}) 
                        VALUES ({placeholders}) 
                        ON CONFLICT (id, datasource_id) DO UPDATE SET {updates}
                        WHERE EXCLUDED."lastupdated" > dataelements."lastupdated" 
                           OR dataelements."lastupdated" IS NULL
                    """
                    
                    cursor.execute(sql, vals)
                    conn.commit()
                    processed_count += 1
                    logger.info(f"Successfully synced: {de_id}")
                    
                    time.sleep(0.05) 

                except Exception as e:
                    error_count += 1
                    conn.rollback()
                    # Log the specific error but continue the loop for other IDs
                    logger.error(f"FAILED dataElement {de_id}: {str(e)}")

    logger.info(f"Sync Finished. Processed: {processed_count}, Errors: {error_count}")
    
    # If too many errors occurred, fail the task at the end
    if error_count > (len(data_elements_list) * 0.5):
        raise Exception("Task failed: More than 50% of data elements failed to sync.")