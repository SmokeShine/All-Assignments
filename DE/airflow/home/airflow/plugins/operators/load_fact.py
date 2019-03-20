from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="redshift",
                 sqlstatement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sqlstatement=sqlstatement
        self.table=table
        self.redshift_conn_id=redshift_conn_id

    def execute(self, context):
        formatted_sql="INSERT INTO {} {}".format(self.table,self.sqlstatement)
        logging.info("SQL Generated")
        logging.info(formatted_sql)
        redshift_hook = PostgresHook("redshift")
        redshift_hook.run(formatted_sql)
        logging.info("Data Pushed to fact table")