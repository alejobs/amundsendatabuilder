"""
This is a example script which demo how to load data
into Neo4j and Elasticsearch without using an Airflow DAG.
"""

import textwrap

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
    user = 'xxxxx'
    password = 'xxxxxx'
    host = 'xxxxx'
    port = '5439'
    db = 'xxxxx'
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
        sql = textwrap.dedent("""WITH predicate_column_info as (
                    SELECT ns.nspname AS schema_name, c.relname AS table_name, a.attnum as col_num,  a.attname as col_name, s.stanullfrac as pct_null,
                    s.stawidth as avg_width, case when s.stadistinct < 0 then null else s.stadistinct end as n_distinct,
                           a.attisdistkey, a.attsortkeyord,
                            CASE
                                WHEN 10002 = s.stakind1 THEN array_to_string(stavalues1, '||')
                                WHEN 10002 = s.stakind2 THEN array_to_string(stavalues2, '||')
                                WHEN 10002 = s.stakind3 THEN array_to_string(stavalues3, '||')
                                WHEN 10002 = s.stakind4 THEN array_to_string(stavalues4, '||')
                                ELSE NULL::varchar
                            END AS pred_ts
                       FROM pg_statistic s
                       JOIN pg_class c ON c.oid = s.starelid
                       JOIN pg_namespace ns ON c.relnamespace = ns.oid
                       JOIN pg_attribute a ON c.oid = a.attrelid AND a.attnum = s.staattnum),
                    now_epoch AS (
                        SELECT FLOOR(DATE_PART(epoch , getdate())) AS now_epoch
                    )
                    SELECT 'redshift' AS cluster, 'redshift' AS db, schema_name || '.' || table_name AS table_name, col_name,
                    '%% null' AS stat_name, NVL(pct_null,-1) AS stat_val, now_epoch.now_epoch AS start_epoch, now_epoch.now_epoch AS end_epoch
                    FROM predicate_column_info
                    CROSS JOIN now_epoch
                    where pred_ts NOT LIKE '2000-01-01%%'
                    AND schema_name IN ('dwh', 'source')
                    UNION
                    SELECT 'redshift' AS cluster, 'redshift' AS db, schema_name || '.' || table_name AS table_name, col_name,
                    '%% avg width' AS stat_name, NVL(avg_width,-1) AS stat_val, now_epoch.now_epoch AS start_epoch, now_epoch.now_epoch AS end_epoch
                    FROM predicate_column_info
                    CROSS JOIN now_epoch
                    where pred_ts NOT LIKE '2000-01-01%%'
                    AND schema_name IN ('dwh', 'source')
                    UNION
                    SELECT 'redshift' AS cluster, 'redshift' AS db, schema_name || '.' || table_name AS table_name, col_name,
                    'distinct' AS stat_name, NVL(n_distinct,-1) AS stat_val, now_epoch.now_epoch AS start_epoch, now_epoch.now_epoch AS end_epoch
                    FROM predicate_column_info
                    CROSS JOIN now_epoch
                    where pred_ts NOT LIKE '2000-01-01%%'
                    AND schema_name IN ('dwh', 'source');
                    """)

        # start usage job
        job_col_usage = create_sample_job(sql, 'test_column_stats',
                                          'databuilder.models.table_stats.TableColumnStats')
        job_col_usage.launch()
