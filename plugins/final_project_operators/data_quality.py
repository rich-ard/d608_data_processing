from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = '',
                 tests=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id = self.redshift_conn_id)   
        for test in self.tests:
            table = test.get("table")
            result = test.get("return")
            
            records = postgres.get_records(table)[0]
            if records[0] == result:
                self.log.info("Pass data QC")
            else:
                self.log.info("Fail data QC")
        self.log.info("Data QC complete!")
        # self.log.info('DataQualityOperator not implemented yet')