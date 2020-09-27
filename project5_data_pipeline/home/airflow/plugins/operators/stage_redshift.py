from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

from airflow.hooks.S3_hook import S3Hook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        TIMEFORMAT AS 'epochmillisecs'
        region 'us-west-2'
    """


    @apply_defaults
    def __init__(self,
                 
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 json_value="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3 = S3Hook(aws_conn_id=self.redshift_conn_id, verify=False)
        self.credentials = self.s3.get_credentials()
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.json_value = json_value

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
                
        self.log.info("Copying data from S3 bucket to Redshift")
        rendered_key = self.s3_key.format(**context)
        self.log.info(rendered_key)
        s3_path = "{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table, 
            s3_path, 
            credentials.access_key, 
            credentials.secret_key, 
            self.json_value
        )
        
        postgres_hook.run(formatted_sql)
        





