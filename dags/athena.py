from airflow.models import DAG
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from datetime import datetime


class XComEnabledAWSAthenaOperator(AWSAthenaOperator):
    def execute(self, context):
        super(XComEnabledAWSAthenaOperator, self).execute(context)
        # just so that this gets `xcom_push`(ed)
        return self.query_execution_id


with DAG(dag_id='athena_query_and_move',
         schedule_interval=None,
         start_date=datetime(2019, 6, 7)) as dag:

    run_query = XComEnabledAWSAthenaOperator(
        task_id='run_query',
        query='SELECT * FROM  UNNEST(SEQUENCE(0, 100))',
        output_location='s3://my-bucket/my-path/',
        database='my_database')

    move_results = S3FileTransformOperator(
        task_id='move_results',
        source_s3_key=
        's3://mybucket/mypath/{{ task_instance.xcom_pull(task_ids="run_query") }}.csv',
        dest_s3_key='s3://mybucket/otherpath/myresults.parquet',
        transform_script='csv_to_parquet.py')

move_results.set_upstream(run_query)