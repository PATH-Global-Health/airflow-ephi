from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

# Import functions
from eth_dhis2_tasks.data_elements import sync_data_elements
from eth_dhis2_tasks.category_combo import sync_category_combos
from eth_dhis2_tasks.category_option_combo import sync_category_option_combos
from eth_dhis2_tasks.category_dimension import build_category_dimension
from eth_dhis2_tasks.org_units import sync_org_units
from eth_dhis2_tasks.transform_org_units import transform_org_units
from eth_dhis2_tasks.data_sets import sync_datasets
from eth_dhis2_tasks.attribute_combo import sync_attribute_combos
from eth_dhis2_tasks.attribute_option_combo import sync_attribute_option_combos
from eth_dhis2_tasks.attribute_dimension import build_attribute_dimension

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Ethiopia-HMIS-Metadata-ETL',
    default_args=default_args,
    description='Ethiopia HMIS DHIS2 Metadata Pipeline',
    schedule=None, 
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=['ETHIOPIA', 'DHIS2', 'HMIS', 'Metadata']
) as dag:

    # --- SHARED CONFIG ---
    eth_creds = {
        'URL': Variable.get("ETH_HMIS_URL"),
        'USERNAME': Variable.get("ETH_HMIS_USERNAME"),
        'PASSWORD': Variable.get("ETH_HMIS_PASSWORD"),
        "STAGING_SCHEMA_NAME": "stg_hmis",
        "POSTGRES_CONN_ID": "PG-ETH-DW",
        "DATA_SOURCE": "hmis"
    }

    # --- TASK DEFINITIONS ---

    # 1. Sync DataElements
    sync_de_task = PythonOperator(
        task_id='sync_data_elements',
        python_callable=sync_data_elements,
        op_kwargs={
            **eth_creds,
            'DATA_ELEMENTS': Variable.get("ETH_HMIS_DATA_ELEMENTS", deserialize_json=True)
        }
    )

    # 2. Sync Datasets
    sync_ds_task = PythonOperator(
        task_id='sync_datasets',
        python_callable=sync_datasets,
        op_kwargs={
            **eth_creds,
            'DATA_ELEMENTS': Variable.get("ETH_HMIS_DATA_ELEMENTS", deserialize_json=True)
        }
    )

    # 3-5. Category Metadata Branch
    sync_cat_combos_task = PythonOperator(
        task_id='sync_category_combos',
        python_callable=sync_category_combos,
        op_kwargs=eth_creds
    )

    sync_cat_option_combos_task = PythonOperator(
        task_id='sync_category_option_combos',
        python_callable=sync_category_option_combos,
        op_kwargs=eth_creds
    )

    build_cat_dim_task = PythonOperator(
        task_id='build_category_dimension',
        python_callable=build_category_dimension,
        op_kwargs={
            "POSTGRES_CONN_ID": "PG-ETH-DW", 
            "STAGING_SCHEMA_NAME": "stg_hmis", 
            "DATA_SOURCE": "hmis"
        }
    )

    # 6-8. Attribute Metadata Branch
    sync_att_combos_task = PythonOperator(
        task_id='sync_attribute_combos',
        python_callable=sync_attribute_combos,
        op_kwargs=eth_creds
    )

    sync_att_option_combos_task = PythonOperator(
        task_id='sync_attribute_option_combos',
        python_callable=sync_attribute_option_combos,
        op_kwargs=eth_creds
    )

    build_att_dim_task = PythonOperator(
        task_id='build_attribute_dimension',
        python_callable=build_attribute_dimension,
        op_kwargs={"POSTGRES_CONN_ID": "PG-ETH-DW", "STAGING_SCHEMA_NAME": "stg_hmis", "DATA_SOURCE": "hmis"}
    )

    # 9. Org Units
    sync_org_units_task = PythonOperator(
        task_id='sync_org_units',
        python_callable=sync_org_units,
        op_kwargs=eth_creds
    )

    # 10. Org Unit Transformation
    transform_org_units_task = PythonOperator(
        task_id='transform_org_units',
        python_callable=transform_org_units,
        op_kwargs={
            'ROOT_ORGUNIT_IDs': Variable.get("ETH_HMIS_ROOT_ORGUNIT_IDs", deserialize_json=True), 
            "POSTGRES_CONN_ID": "PG-ETH-DW",
            "STAGING_SCHEMA_NAME": "stg_hmis"
        }
    )

    # --- DEPENDENCIES (THE DAG GRAPH) ---

    # Start with Data Elements, then Datasets
    sync_de_task >> sync_ds_task

    # After Datasets, split into Parallel Branches
    sync_ds_task >> [sync_cat_combos_task, sync_att_combos_task]

    # Category Branch Flow
    sync_cat_combos_task >> sync_cat_option_combos_task >> build_cat_dim_task

    # Attribute Branch Flow
    sync_att_combos_task >> sync_att_option_combos_task >> build_att_dim_task

    # Fan-in: Both branches must complete before Org Units start
    [build_cat_dim_task, build_att_dim_task] >> sync_org_units_task

    # Final Step
    sync_org_units_task >> transform_org_units_task