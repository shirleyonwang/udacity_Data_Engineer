from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                 aws_credentials_id="",
                 table_names = [],  
                 dq_checks =[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_names = table_names     
        self.dq_checks = dq_checks
      
        self.aws_credentials_id = aws_credentials_id

        
    def execute(self, context):
        self.log.info('DataQualityOperator is implementing......')
          
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        for table in self.table_names:
            records = redshift.get_records(f"Select count(*) from {table}")
            self.log.info('DataQualityOperator checking for row count')
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} retuned no results")
        
        
        for check in self.dq_checks:
             records = redshift.get_records(check['check_sql'])[0]
             self.log.info('DataQualityOperator checking for Null ids')
             if records[0] != check['expected_result']:
                raise ValueError(f"Data quality check failed. {check['table']} contains null in id column, got {records[0]} instead")
