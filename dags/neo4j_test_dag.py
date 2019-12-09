from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from operators.neo4j_csv_s3_operator import Neo4jCsvS3Operator
import logging

default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'start_date': datetime(2018, 1, 1),
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=5),
}

QUERY = 'MATCH(f:Form) RETURN f{.*} LIMIT 50'

with DAG('neo4j_test_dag',
		 max_active_runs=3,
		 schedule_interval='@once',
		 default_args=default_args) as dag:

	t1 = BashOperator(
		task_id='print_date',
		bash_command='date',
		dag=dag)

	nop = Neo4jCsvS3Operator(
		task_id='neo4j_query',
		query_type='READ',
		query=QUERY,
		query_args= {
			'companyid':1
		},
		record_key='f',
		field_to_header_map={
			"sysid":"SYSID",
			"formNumber":"FORM_NUMBER"
		},
		aws_conn_id="aws_s3_default",
		s3_dest_key="test.csv",
		s3_bucket="systum-local",
		s3_dest_verify=True,
		s3_replace=True,
		dag=dag
	)


	#t1 >> nop
	nop.set_upstream(t1)

