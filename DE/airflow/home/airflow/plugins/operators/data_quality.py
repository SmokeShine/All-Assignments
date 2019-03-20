from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables=[],
                 redshift_conn_id="redshift",
                 custom_check={'test'      : 'SELECT COUNT(*) FROM songs WHERE COLUMN IS NULL',
                               'expected'  : 0},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables=tables
        self.redshift_conn_id=redshift_conn_id
        self.custom_check=custom_check
    def execute(self, context,*args, **kwargs):
        logging.info("Starting Data Quality Check")
        for table in self.tables:
            
            redshift_hook = PostgresHook("redshift")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        
        logging.info("Running custom sql passed")
        custom_sql = self.custom_check["test"]
        answer = self.custom_check["expected"]
        logging.info(custom_sql)
        records = redshift_hook.get_records(custom_sql)
        num_records = records[0][0]
        logging.info(num_records)        
        if (answer==num_records):
            logging.info("Test for custom SQL Passed")
        else:
            logging.info(f"Mismatch - {answer} != {num_records}")
                   