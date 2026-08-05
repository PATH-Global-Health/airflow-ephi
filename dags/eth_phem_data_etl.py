from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# from eth_dhis2_tasks.dataset_completeness import sync_completeness
from eth_dhis2_tasks.data_values import sync_data_values
from eth_dhis2_tasks.reporting_rates import sync_reporting_rates
from eth_dhis2_tasks.periods import sync_periods
from eth_dhis2_tasks.prepare_full_rebuild import prepare_full_rebuild

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Ethiopia-PHEM-Data-ETL',
    default_args=default_args,
    description='Ethiopia PHEM DHIS2 Data Pipeline',
    # Minute 0, Hour 21 (9PM), Day of Month *, Month *, Day of Week 3 (Wednesday)
    schedule='0 21 * * 3',
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=['ETHIOPIA', 'DHIS2', 'PHEM', 'Data']
) as dag:
    
     # --- SHARED CONFIG ---
    eth_creds = {
        'URL': Variable.get("ETH_PHEM_URL"),
        'USERNAME': Variable.get("ETH_PHEM_USERNAME"),
        'PASSWORD': Variable.get("ETH_PHEM_PASSWORD"),
        "POSTGRES_CONN_ID": "PG-ETH-DW",
        "DATA_SOURCE": "phem"
    }

    # Sync tuning (adjustable via Airflow Variables without code changes)
    max_workers      = int(Variable.get("ETH_PHEM_MAX_WORKERS",      default_var="6"))
    calls_per_second = float(Variable.get("ETH_PHEM_CALLS_PER_SECOND", default_var="5"))
    rr_batch_size    = int(Variable.get("ETH_PHEM_RR_BATCH_SIZE",     default_var="7"))

    # Sync data values
    dv_download_all = Variable.get("ETH_PHEM_DOWNLOAD_ALL_DATA_VALUES", default_var="False")
    sync_dv_task = PythonOperator(
        task_id='sync_data_values',
        python_callable=sync_data_values,
        execution_timeout=timedelta(hours=120),
        op_kwargs={
            **eth_creds,
            'DATA_ELEMENTS': Variable.get("ETH_PHEM_DATA_ELEMENTS", deserialize_json=True),
            'DEFAULT_START': Variable.get("ETH_PHEM_DEFAULT_DOWNLOAD_START_DATE"),
            'DOWNLOAD_ALL': str(dv_download_all).lower() in ['true', '1', 't', 'y', 'yes'],
            'USE_ETHIOPIAN_CALENDAR': False,
            'MAX_WORKERS': max_workers,
            'CALLS_PER_SECOND': calls_per_second,
        }
    )

    # Sync periods
    sync_periods_task = PythonOperator(
        task_id='sync_periods',
        python_callable=sync_periods,
        execution_timeout=timedelta(hours=48),
        op_kwargs={
            **eth_creds
        }
    )

    # Sync dataset completeness
    # dc_download_all = Variable.get("ETH_PHEM_FULL_REBUILD_COMPLETENESS")
    # prepare_full_rebuild_dc_task = PythonOperator(
    #     task_id='prepare_full_rebuild_completeness',
    #     python_callable=prepare_full_rebuild,
    #     op_kwargs={
    #         **eth_creds,
    #         'FULL_REBUILD': str(dc_download_all).lower() in ['true', '1', 't', 'y', 'yes'],
    #         'TARGET_TABLE': 'phem_dataset_completeness',
    #         'DOWNLOADED_AT_FIELD': 'dc_downloadedat'
    #     }
    # )
    # sync_dataset_compl_task = PythonOperator(
    #     task_id='sync_dataset_completeness',
    #     python_callable=sync_completeness,
    #     execution_timeout=timedelta(hours=120),
    #     op_kwargs={
    #         **eth_creds,
    #         'FULL_REBUILD': str(dc_download_all).lower() in ['true', '1', 't', 'y', 'yes'],
    #         'ROOT_ORGUNIT_IDs': Variable.get("ETH_PHEM_ROOT_ORGUNIT_IDs", deserialize_json=True), 
    #         "STAGING_SCHEMA_NAME": "stg_phem"
    #     }
    # )

    # Sync reporting rate
    rr_download_all = Variable.get("ETH_PHEM_FULL_REBUILD_REPORTING_RATE")
    prepare_full_rebuild_rr_task = PythonOperator(
        task_id='prepare_full_rebuild_reporting_rate',
        python_callable=prepare_full_rebuild,
        op_kwargs={
            **eth_creds,
            'FULL_REBUILD': str(rr_download_all).lower() in ['true', '1', 't', 'y', 'yes'],
            'TARGET_TABLE': 'phem_reporting_rates',
            'DOWNLOADED_AT_FIELD': 'rr_downloadedat'
        }
    )

    sync_rr_task = PythonOperator(
        task_id='sync_reporting_rates',
        python_callable=sync_reporting_rates,
        execution_timeout=timedelta(hours=120),
        op_kwargs={
            **eth_creds,
            'FULL_REBUILD': str(rr_download_all).lower() in ['true', '1', 't', 'y', 'yes'],
            'ROOT_ORGUNIT_IDs': Variable.get("ETH_PHEM_ROOT_ORGUNIT_IDs", deserialize_json=True),
            'STAGING_SCHEMA_NAME': "stg_phem",
            'MAX_WORKERS': max_workers,
            'CALLS_PER_SECOND': calls_per_second,
            'BATCH_SIZE': rr_batch_size,
        }
    )

    # Delete ghost datavalues
    delete_ghost_datavalue = SQLExecuteQueryOperator(
        task_id='delete_ghost_datavalues',
        conn_id='PG-ETH-DW',
        sql="""
            DELETE FROM public.phem_datavalues dv
            WHERE NOT EXISTS (
                SELECT 1 
                FROM public.dataelements de
                -- Join to find the Category Combo assigned to the Data Element
                JOIN stg_phem.categoryoptioncombos coc 
                ON de.categorycombo_id = coc.categorycombo_id
                WHERE de.id = dv.dataelement 
                AND coc.id = dv.categoryoptioncombo
            );
        """
    )

    # Dependency Chain
    sync_dv_task >> sync_periods_task >> delete_ghost_datavalue
    # # Branch A: Completeness
    # delete_ghost_datavalue >> prepare_full_rebuild_dc_task >> sync_dataset_compl_task
    # Branch B: Reporting Rates
    delete_ghost_datavalue >> prepare_full_rebuild_rr_task >> sync_rr_task