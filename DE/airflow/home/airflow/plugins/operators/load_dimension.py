from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="redshift",
                 sqlstatement="",
                 loadtype="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sqlstatement=sqlstatement
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.loadtype=loadtype

    def execute(self, context):
        if self.loadtype=="truncate":
            formatted_sql="TRUNCATE {}; INSERT INTO {} {}".format(self.table,self.table,self.sqlstatement)
        elif self.loadtype=="update":
            formatted_sql="INSERT INTO {} {}".format(self.table,self.sqlstatement)
        logging.info("SQL Generated")
        logging.info(formatted_sql)
        redshift_hook = PostgresHook("redshift")
        redshift_hook.run(formatted_sql)
        logging.info("Data Pushed to Dimension Table")