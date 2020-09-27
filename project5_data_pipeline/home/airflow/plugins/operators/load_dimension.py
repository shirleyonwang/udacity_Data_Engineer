from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id='',
                aws_credentials_id="",
                table_name='', 
                sql='',
                append_data='True',
                
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.aws_credentials_id = aws_credentials_id

        self.table_name= table_name
        self.sql = sql
        self.append_data = append_data

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Appending data')

        if self.append_data == False:
            sql_statement = "DELETE FROM {}".format(self.table_name)
            postgres_hook.run(sql_statement)
            
        
        sql_statement ="Insert into {}  {}".format(self.table_name,self.sql)

        postgres_hook.run(sql_statement)
