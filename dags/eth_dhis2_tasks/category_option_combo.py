import time
import logging
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from eth_dhis2_tasks.utils import DHIS2Session

# Set up logging for Airflow
logger = logging.getLogger(__name__)

def sync_category_option_combos(**kwargs):
    url = kwargs["URL"].rstrip("/")
    username = kwargs["USERNAME"]
    password = kwargs["PASSWORD"]
    pg_conn_id = kwargs.get("POSTGRES_CONN_ID")
    staging = kwargs["STAGING_SCHEMA_NAME"]
    
    pg = PostgresHook(postgres_conn_id=pg_conn_id)
    
    # Initialize session
    try:
        dhis = DHIS2Session(url, username, password)
    except Exception as e:
        logger.error(f"Failed to initialize DHIS2 Session: {e}")
        raise

    PAGE_SIZE = 250 

    # Extract Category Combo IDs from Data Elements
    # Using unnest to handle comma-separated values in the dataelements table
    get_ids_sql = """
        SELECT DISTINCT trim(unnest(string_to_array(categorycombo_id, ','))) as ccid
        FROM dataelements
        WHERE categorycombo_id IS NOT NULL AND categorycombo_id != '';
    """
    
    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(get_ids_sql)
            combo_ids = [row[0] for row in cursor.fetchall() if row[0]]
            
    if not combo_ids:
        logger.info("No CategoryCombo IDs found in dataelements to process COCs.")
        return

    all_cocs = {} 
    all_coc_option_links = set() 
    fields = "id,name,code,created,lastUpdated,categoryCombo[id],categoryOptions[id]"

    # Paged Download for the Category-specific IDs
    logger.info(f"Syncing COCs for {len(combo_ids)} Category-linked combos...")

    for ccid in combo_ids:
        page = 1
        while True:
            params = {
                "fields": fields,
                "filter": f"categoryCombo.id:eq:{ccid}",
                "paging": "true",
                "pageSize": str(PAGE_SIZE),
                "page": str(page)
            }
            try:
                data = dhis.get("api/categoryOptionCombos.json", params=params)
                
                items = data.get("categoryOptionCombos", [])
                if not items:
                    break

                for coc in items:
                    # Tuple order: id, name, code, categorycombo_id, created, lastupdated
                    all_cocs[coc['id']] = (
                        coc.get("id"),
                        coc.get("name"),
                        coc.get("code"),
                        (coc.get("categoryCombo") or {}).get("id"),
                        coc.get("created"),
                        coc.get("lastUpdated")
                    )
                    for opt in (coc.get("categoryOptions") or []):
                        all_coc_option_links.add((coc.get("id"), opt.get("id")))

                pager = data.get("pager", {})
                if page >= pager.get("pageCount", 0):
                    break
                page += 1
                time.sleep(0.05)
            except Exception as e:
                logger.error(f"Error fetching Category COCs for combo {ccid}: {e}")
                break

    # Atomic Database Update
    if not all_cocs:
        logger.warning("No Category Option Combos were retrieved. Aborting DB sync.")
        return

    try:
        with pg.get_conn() as conn:
            with conn.cursor() as cursor:
                logger.info(f"Upserting {len(all_cocs)} Category COCs into {staging}...")

                # 1: Upsert COC entities (prevents duplicate key errors for shared 'default' COC)
                coc_upsert_sql = f"""
                    INSERT INTO {staging}.categoryoptioncombos (
                        id, name, code, categorycombo_id, created, lastupdated
                    ) VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        code = EXCLUDED.code,
                        categorycombo_id = EXCLUDED.categorycombo_id,
                        lastupdated = EXCLUDED.lastupdated,
                        _fetchedat = CURRENT_TIMESTAMP;
                """
                execute_values(cursor, coc_upsert_sql, list(all_cocs.values()))

                # 2: Relationship Bridge Sync
                # IMPORTANT: Delete only bridge links where the parent combo is 'category' type
                if all_coc_option_links:
                    logger.info("Refreshing COC-Option bridge links for category context...")
                    cursor.execute(f"""
                        DELETE FROM {staging}.categoryoptioncombo_options 
                        WHERE categoryoptioncombo_id IN (
                            SELECT coc.id 
                            FROM {staging}.categoryoptioncombos coc
                            JOIN {staging}.categorycombos cc ON coc.categorycombo_id = cc.id
                            WHERE cc.combo_type = 'category'
                        )
                    """)

                    link_sql = f"INSERT INTO {staging}.categoryoptioncombo_options (categoryoptioncombo_id, categoryoption_id) VALUES %s"
                    execute_values(cursor, link_sql, list(all_coc_option_links))

                conn.commit()
                logger.info(f"Sync Complete: {len(all_cocs)} Category COCs updated.")

    except Exception as e:
        logger.error(f"Database sync failed for Category COCs: {e}")
        raise