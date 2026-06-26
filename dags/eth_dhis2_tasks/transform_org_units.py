from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_org_units(**kwargs):
    """
    Transforms raw organization unit data from the staging schema into a flattened 
    hierarchy within the public warehouse schema. Supports multi-source (datasource_id)
    logic to prevent ID collisions and ensure data lineage.
    """
    pg_conn_id = kwargs.get("POSTGRES_CONN_ID")
    PARENT_ROOT_IDS = kwargs["ROOT_ORGUNIT_IDs"]
    staging = kwargs["STAGING_SCHEMA_NAME"]
    
    pg = PostgresHook(postgres_conn_id=pg_conn_id)
    
    # Prepare the SQL for ID extraction from the Path
    # DHIS2 paths look like /id1/id2/id3. 
    # split_part(path, '/', 2) gets the first ID, and so on.
    level_logic = ""
    for i in range(1, 9):
        # Extract the ID at this level from the path string
        level_logic += f", split_part(path, '/', {i+1}) as oulevel{i}id \n"

    # Build Root Filter
    # This ensures we only process orgunits belonging to specific parent trees if required
    root_filter = ""
    if PARENT_ROOT_IDS:
        ids_str = ",".join([f"'{x}'" for x in PARENT_ROOT_IDS]) # Format for SQL IN clause
        # Filter: keep if any ID in the path is in our PARENT_ROOT_IDS list
        root_filter = f"""
            WHERE EXISTS (
                SELECT 1 FROM unnest(string_to_array(path, '/')) as x 
                WHERE x IN ({ids_str})
            )
        """

    # Build the Full Transformation Query
    # We use a CTE (Common Table Expression) to extract IDs, then JOIN back 
    # to the raw table to get names for each level.
    # Note: Joins now include datasource_id to ensure we don't cross-pollinate 
    # names between HMIS and PHEM systems.

    

    sql = f"""
    WITH base_ids AS (
        SELECT *, 
        string_to_array(trim(both '/' from path), '/') as ids_array
        {level_logic}
        FROM {staging}.orgunits
        {root_filter}
    )
    INSERT INTO orgunits (
        id, datasource_id, code, name, shortname, parentid, oupath, level,
        openingdate, closeddate, lastupdated, _fetchedat, leaf,
        oulevel1id, oulevel1name, oulevel2id, oulevel2name,
        oulevel3id, oulevel3name, oulevel4id, oulevel4name,
        oulevel5id, oulevel5name, oulevel6id, oulevel6name,
        oulevel7id, oulevel7name, oulevel8id, oulevel8name
    )
    SELECT 
        b.id, b.datasource_id, b.code, b.name, b.shortname, b.parentid, b.path, b.level,
        b.openingdate, b.closeddate, b.lastupdated, b._fetchedat, b.leaf,
        b.oulevel1id, l1.name as oulevel1name,
        b.oulevel2id, l2.name as oulevel2name,
        b.oulevel3id, l3.name as oulevel3name,
        b.oulevel4id, l4.name as oulevel4name,
        b.oulevel5id, l5.name as oulevel5name,
        b.oulevel6id, l6.name as oulevel6name,
        b.oulevel7id, l7.name as oulevel7name,
        b.oulevel8id, l8.name as oulevel8name
    FROM base_ids b
    -- Contextual Joins: We must match both ID and datasource_id to find the correct parent names
    LEFT JOIN {staging}.orgunits l1 ON b.oulevel1id = l1.id AND b.datasource_id = l1.datasource_id
    LEFT JOIN {staging}.orgunits l2 ON b.oulevel2id = l2.id AND b.datasource_id = l2.datasource_id
    LEFT JOIN {staging}.orgunits l3 ON b.oulevel3id = l3.id AND b.datasource_id = l3.datasource_id
    LEFT JOIN {staging}.orgunits l4 ON b.oulevel4id = l4.id AND b.datasource_id = l4.datasource_id
    LEFT JOIN {staging}.orgunits l5 ON b.oulevel5id = l5.id AND b.datasource_id = l5.datasource_id
    LEFT JOIN {staging}.orgunits l6 ON b.oulevel6id = l6.id AND b.datasource_id = l6.datasource_id
    LEFT JOIN {staging}.orgunits l7 ON b.oulevel7id = l7.id AND b.datasource_id = l7.datasource_id
    LEFT JOIN {staging}.orgunits l8 ON b.oulevel8id = l8.id AND b.datasource_id = l8.datasource_id
    
    -- Compound Key UPSERT: Handle potential collisions safely
    ON CONFLICT (id, datasource_id) DO UPDATE SET
        code = EXCLUDED.code,
        name = EXCLUDED.name,
        oupath = EXCLUDED.oupath,
        level = EXCLUDED.level,
        lastupdated = EXCLUDED.lastupdated,
        oulevel1name = EXCLUDED.oulevel1name,
        oulevel2name = EXCLUDED.oulevel2name,
        oulevel3name = EXCLUDED.oulevel3name,
        oulevel4name = EXCLUDED.oulevel4name,
        oulevel5name = EXCLUDED.oulevel5name,
        oulevel6name = EXCLUDED.oulevel6name,
        oulevel7name = EXCLUDED.oulevel7name,
        oulevel8name = EXCLUDED.oulevel8name;
        -- Note: datasource_id and id are excluded from UPDATE because they are the conflict target
    """

    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            # We don't need to truncate since we use ON CONFLICT (Upsert)
            cursor.execute(sql)
            conn.commit()
            print("Successfully processed Organisation Unit Hierarchy into warehouse with compound keys.")