from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.neo4j_hook import Neo4jHook
from airflow.hooks.S3_hook import S3Hook
from tempfile import NamedTemporaryFile
import logging, csv, io

logger = logging.getLogger(__name__)

class Neo4jCsvS3Operator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 query_type,
                 query,
                 query_args,
                 record_key,
                 field_to_header_map,
                 aws_conn_id,
                 s3_bucket,
                 s3_dest_key,
                 s3_dest_verify,
                 s3_replace,
                 *args,
                 **kwargs):
        super(Neo4jCsvS3Operator, self).__init__(*args, **kwargs)
        self.query_type = query_type
        self.query = query
        self.query_args = query_args
        self.record_key = record_key
        self.field_to_header_map = field_to_header_map
        self.aws_conn_id = aws_conn_id
        self.s3_dest_key = s3_dest_key
        self.s3_bucket = s3_bucket
        self.s3_dest_verify = s3_dest_verify
        self.s3_replace = s3_replace

    def execute(self, context):
        hook = Neo4jHook()
        if self.query_type == 'WRITE':
            driver = hook.get_write_driver()
        else:
            driver = hook.get_read_driver()
        with driver.session() as session:
            objs = []
            tx = session.begin_transaction()
            keys = self.field_to_header_map.keys()
            try:
                results = tx.run(self.query, self.query_args)
                for record in results:
                    data = record.get(self.record_key)
                    if data:
                        obj = {}
                        for k in keys:
                            obj[self.field_to_header_map[k]] = data.get(k)
                        objs.append(obj)
            except Exception as e:
                logger.error("QUERY ERROR: {}".format(e))

            #logger.info(self.write_to_csv(objs))
            self.load_in_s3(self.write_to_csv(objs))

    def load_in_s3(self,content):
        dest_s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.s3_dest_verify)
        with NamedTemporaryFile("wb",dir="/tempcsv",delete=True) as f_dest:
            f_dest.write(content.encode("utf-8"))
            f_dest.flush()
            dest_s3.load_file(
                filename=f_dest.name,
                bucket_name=self.s3_bucket,
                key=self.s3_dest_key,
                replace=self.s3_replace
            )

    def write_to_csv(self,objs):
        output = io.StringIO()
        with output as csvfile:
            headers = self.field_to_header_map.values()
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for o in objs:
                writer.writerow(o)
            return output.getvalue()
