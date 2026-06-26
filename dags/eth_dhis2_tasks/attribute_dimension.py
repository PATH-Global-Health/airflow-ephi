import unicodedata
import re
import logging
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

def sanitize(name: str) -> str:
    """Standardizes DHIS2 names into safe Postgres column names."""
    if name is None: return "col"
    s = unicodedata.normalize('NFKD', name)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.strip().lower()
    s = re.sub(r'[ ,;{}()\n\t=]+', '_', s)
    s = re.sub(r'[^a-z0-9_]', '_', s)
    s = re.sub(r'_+', '_', s).strip('_')
    if re.match(r'^\d', s or ''): s = f"_{s}"
    return s[:63] if s else "col"

def build_attribute_dimension(**kwargs):
    """Builds source-specific attribute dimension (e.g., hmis_attributes)."""
    pg_conn_id = kwargs.get("POSTGRES_CONN_ID")
    staging = kwargs["STAGING_SCHEMA_NAME"]
    datasource = kwargs["DATA_SOURCE"].lower()
    target_table = f"{datasource}_attributes"
    
    pg = PostgresHook(postgres_conn_id=pg_conn_id)
    
    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            # Fetch metadata specifically for 'attribute' context
            fetch_long_sql = f"""
                SELECT DISTINCT
                    cat.name as cat_name,
                    opt.name as opt_name,
                    coc.id as coc_id,
                    coc.name as coc_name,
                    coc.code as coc_code,
                    coc.categorycombo_id
                FROM {staging}.categoryoptioncombos coc
                JOIN {staging}.categoryoptioncombo_options link ON coc.id = link.categoryoptioncombo_id
                JOIN {staging}.categoryoptions opt ON link.categoryoption_id = opt.id AND opt.combo_type = 'attribute'
                JOIN {staging}.category_categoryoptions cor ON opt.id = cor.categoryoption_id
                JOIN {staging}.categories cat ON cor.category_id = cat.id AND cat.combo_type = 'attribute'
                WHERE cat.name IS NOT NULL 
            """
            cursor.execute(fetch_long_sql)
            rows = cursor.fetchall()
            
            if not rows:
                logger.warning(f"No attribute data found in {staging}. Skipping build.")
                return

            # Generate Safe Mapping
            distinct_cats = sorted(list(set(row[0] for row in rows)))
            mapping = {}
            used_columns = set()
            for orig in distinct_cats:
                base = sanitize(orig)
                cand, i = base, 2
                while cand in used_columns:
                    cand = f"{base}_{i}"
                    i += 1
                mapping[orig] = cand
                used_columns.add(cand)

            # Update the mapping table in staging
            map_data = [(k, v) for k, v in mapping.items()]
            execute_values(cursor, f"""
                INSERT INTO {staging}.attribute_name_map (original_name, safe_name) 
                VALUES %s 
                ON CONFLICT (original_name) DO UPDATE SET 
                    safe_name = EXCLUDED.safe_name,
                    _builtat = CURRENT_TIMESTAMP;
            """, map_data)

            # Build Dynamic Pivot SQL
            pivot_columns = []
            for orig, safe in mapping.items():
                orig_escaped = orig.replace("'", "''")
                pivot_columns.append(f"MAX(CASE WHEN cat.name = '{orig_escaped}' THEN opt.name END) AS \"{safe}\"")

            logger.info(f"Building warehouse table '{target_table}' with {len(mapping)} columns.")

            dynamic_sql = f"""
                DROP TABLE IF EXISTS {target_table} CASCADE;
                
                CREATE TABLE {target_table} AS
                SELECT 
                    '{datasource}'::TEXT as datasource_id,
                    coc.id as categoryoptioncombo_id, 
                    coc.name as categoryoptioncombo_name, 
                    coc.code as categoryoptioncombo_code,
                    {', '.join(pivot_columns)},
                    coc.categorycombo_id,
                    NOW() as _builtat
                FROM {staging}.categoryoptioncombos coc
                JOIN {staging}.categoryoptioncombo_options link ON coc.id = link.categoryoptioncombo_id
                JOIN {staging}.categoryoptions opt ON link.categoryoption_id = opt.id AND opt.combo_type = 'attribute'
                JOIN {staging}.category_categoryoptions cor ON opt.id = cor.categoryoption_id
                JOIN {staging}.categories cat ON cor.category_id = cat.id AND cat.combo_type = 'attribute'
                GROUP BY coc.id, coc.name, coc.code, coc.categorycombo_id;
                
                CREATE INDEX idx_{target_table}_coc_id ON {target_table}(categoryoptioncombo_id);
            """
            
            cursor.execute(dynamic_sql)
            conn.commit()
            logger.info(f"Dimension table '{target_table}' built successfully.")