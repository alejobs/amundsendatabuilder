"""
This is a example script which demo how to load data
into Neo4j and Elasticsearch without using an Airflow DAG.
"""

import textwrap
import psycopg2
import logging

from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory

from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer

# change to the address of Elasticsearch service
es = Elasticsearch([
    {'host': 'localhost'},
])

def connection_string():
    user = 'admin'
    password = 'xxxxxxxxx'
    host = 'xxxxxxxx'
    port = '5439'
    db = 'xxxxxx'
    return "postgresql://%s:%s@%s:%s/%s" % (user, password, host, port, db)

# replace localhost with docker host ip
# todo: get the ip from input argument
NEO4J_ENDPOINT = 'bolt://localhost:7687'
neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = 'neo4j'
neo4j_password = 'test'

# todo: Add a second model
def create_sample_job(sql, table_name, model_name, transformer=NoopTransformer()):

    tmp_folder = '/var/tmp/amundsen/{table_name}'.format(table_name=table_name)
    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

    sql_extractor = SQLAlchemyExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=sql_extractor,
                       loader=csv_loader,
                       transformer=transformer)

    job_config = ConfigFactory.from_dict({
        'extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING): connection_string(),
        'extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.EXTRACT_SQL): sql,
        'extractor.sqlalchemy.model_class': model_name,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
            node_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
            relationship_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR):
            True,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
            node_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
            neo4j_endpoint,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
            neo4j_user,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
            neo4j_password,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG):
            'unique_tag',  # should use unique tag here like {ds}
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())
    return job


if __name__ == "__main__":
    # Uncomment next line to get INFO level logging
    # logging.basicConfig(level=logging.INFO)

    if connection_string():
        sql = textwrap.dedent("""select
                    'redshift' AS database, 'redshift' AS cluster, trim(sti.schema) as schema_name, trim(sti.table) as table_name, '*' AS column_name,
                    trim(pu.usename) || '@netquest.com' as user_email, count(*) AS read_count
                    from
                        (select
                            distinct userid, query, tbl
                        from stl_scan) ss
                    join SVV_TABLE_INFO sti on sti.table_id = ss.tbl
                    join pg_user pu on pu.usesysid = ss.userid
                    join stl_query sq on ss.query = sq.query
                    WHERE trim(sq.querytxt) like 'SELECT%%' AND pu.usename != 'admin'
                    GROUP BY sti.database, cluster, schema_name, table_name, column_name, user_email;
                    """)

        # start usage job
        job_col_usage = create_sample_job(sql, 'test_usage_metadata',
                                          'example.models.test_column_usage_model.TestColumnUsageModel')
        job_col_usage.launch()
