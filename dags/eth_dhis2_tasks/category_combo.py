import time
import logging
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from eth_dhis2_tasks.utils import DHIS2Session

# Initialize logger for Airflow UI visibility
logger = logging.getLogger(__name__)

def sync_category_combos(**kwargs):
    url = kwargs["URL"].rstrip("/")
    username = kwargs["USERNAME"]
    password = kwargs["PASSWORD"]
    pg_conn_id = kwargs.get("POSTGRES_CONN_ID")
    staging = kwargs["STAGING_SCHEMA_NAME"]

    pg = PostgresHook(postgres_conn_id=pg_conn_id)
    
    # Initialize DHIS2 Session using Form-based auth
    try:
        dhis = DHIS2Session(url, username, password)
    except Exception as e:
        logger.error(f"Failed to initialize DHIS2 Session: {e}")
        raise

    # Extract Distinct Combo IDs from Data Elements
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
        logger.info("No categoryCombo IDs found in dataelements. Skipping sync.")
        return

    # Fetch Nested Data from DHIS2
    combos, categories, options = {}, {}, {}
    rel_cc_cat, rel_cat_opt = set(), set()
    fields = (
        "id,name,code,shortName,created,lastUpdated,"
        "categories[id,name,code,shortName,created,lastUpdated,"
        "categoryOptions[id,name,code,shortName,created,lastUpdated]]"
    )
    
    logger.info(f"Fetching metadata for {len(combo_ids)} category combos...")

    for ccid in combo_ids:
        try:
            c = dhis.get(f"api/categoryCombos/{ccid}.json", params={"fields": fields})

            # TUPLE ORDER: id, combo_type, name, code, shortname, created, lastupdated
            combos[c['id']] = (
                c['id'], 'category', c.get('name'), c.get('code'), 
                c.get('shortName'), c.get('created'), c.get('lastUpdated')
            )

            for cat in c.get('categories', []):
                categories[cat['id']] = (
                    cat['id'], 'category', cat.get('name'), cat.get('code'), 
                    cat.get('shortName'), cat.get('created'), cat.get('lastUpdated')
                )
                rel_cc_cat.add((c['id'], cat['id']))

                for opt in cat.get('categoryOptions', []):
                    options[opt['id']] = (
                        opt['id'], 'category', opt.get('name'), opt.get('code'), 
                        opt.get('shortName'), opt.get('created'), opt.get('lastUpdated')
                    )
                    rel_cat_opt.add((cat['id'], opt['id']))
            
            time.sleep(0.05) 
        except Exception as e:
            logger.error(f"Error fetching category combo {ccid}: {e}")

    # Database Persistence (Atomic Staging Upsert)
    if not combos:
        logger.warning("No data was successfully retrieved. Aborting DB sync.")
        return

    try:
        with pg.get_conn() as conn:
            with conn.cursor() as cursor:
                logger.info("Starting atomic database upsert for category metadata...")

                # 1. Entities Upsert (Targeting Compound Key: id, combo_type)
                entity_tables = [
                    ('categorycombos', combos),
                    ('categories', categories),
                    ('categoryoptions', options)
                ]

                for table, data_dict in entity_tables:
                    if data_dict:
                        # ON CONFLICT now targets the compound key specifically
                        insert_sql = f"""
                            INSERT INTO {staging}.{table} (id, combo_type, name, code, shortname, created, lastupdated) 
                            VALUES %s
                            ON CONFLICT (id, combo_type) DO UPDATE SET
                                name = EXCLUDED.name,
                                code = EXCLUDED.code,
                                shortname = EXCLUDED.shortname,
                                lastupdated = EXCLUDED.lastupdated,
                                _fetchedat = CURRENT_TIMESTAMP;
                        """
                        execute_values(cursor, insert_sql, list(data_dict.values()))

                # 2. Relationships Cleanup & Insert
                # We target only links belonging to 'category' context to preserve 'attribute' links
                if rel_cc_cat:
                    logger.info("Refreshing Category-Category relationship links...")
                    cursor.execute(f"""
                        DELETE FROM {staging}.categorycombo_categories 
                        WHERE categorycombo_id IN (
                            SELECT id FROM {staging}.categorycombos WHERE combo_type = 'category'
                        )
                    """)
                    execute_values(
                        cursor, 
                        f"INSERT INTO {staging}.categorycombo_categories (categorycombo_id, category_id) VALUES %s", 
                        list(rel_cc_cat)
                    )

                if rel_cat_opt:
                    logger.info("Refreshing Category-Option relationship links...")
                    cursor.execute(f"""
                        DELETE FROM {staging}.category_categoryoptions 
                        WHERE category_id IN (
                            SELECT id FROM {staging}.categories WHERE combo_type = 'category'
                        )
                    """)
                    execute_values(
                        cursor, 
                        f"INSERT INTO {staging}.category_categoryoptions (category_id, categoryoption_id) VALUES %s", 
                        list(rel_cat_opt)
                    )

                conn.commit()
                logger.info(f"Category Sync Complete: {len(combos)} items persisted to {staging}.")

    except Exception as e:
        logger.error(f"Database transaction failed for category combos: {e}")
        raise