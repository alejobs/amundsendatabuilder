"""
This is a example script which demo how to load data
into Neo4j and Elasticsearch without using an Airflow DAG.
"""

import textwrap
import uuid

from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory

from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer

# change to the address of Elasticsearch service
es = Elasticsearch([
    {'host': 'localhost'},
])

def connection_string():
    user = 'xxxxx'
    password = 'xxxxx'
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





def create_es_publisher_sample_job(elasticsearch_index_alias='table_search_index',
                                   elasticsearch_doc_type_key='table',
                                   model_name='databuilder.models.table_elasticsearch_document.TableESDocument',
                                   cypher_query=None,
                                   elasticsearch_mapping=None):
    """
    :param elasticsearch_index_alias:  alias for Elasticsearch used in
                                       amundsensearchlibrary/search_service/config.py as an index
    :param elasticsearch_doc_type_key: name the ElasticSearch index is prepended with. Defaults to `table` resulting in
                                       `table_search_index`
    :param model_name:                 the Databuilder model class used in transporting between Extractor and Loader
    :param cypher_query:               Query handed to the `Neo4jSearchDataExtractor` class, if None is given (default)
                                       it uses the `Table` query baked into the Extractor
    :param elasticsearch_mapping:      Elasticsearch field mapping "DDL" handed to the `ElasticsearchPublisher` class,
                                       if None is given (default) it uses the `Table` query baked into the Publisher
    """
    # loader saves data to this location and publisher reads it from here
    extracted_search_data_path = '/var/tmp/amundsen/search_data.json'

    task = DefaultTask(loader=FSElasticsearchJSONLoader(),
                       extractor=Neo4jSearchDataExtractor(),
                       transformer=NoopTransformer())

    # elastic search client instance
    elasticsearch_client = es
    # unique name of new index in Elasticsearch
    elasticsearch_new_index_key = 'tables' + str(uuid.uuid4())

    job_config = ConfigFactory.from_dict({
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.GRAPH_URL_CONFIG_KEY): neo4j_endpoint,
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.MODEL_CLASS_CONFIG_KEY): model_name,
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_AUTH_USER): neo4j_user,
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_AUTH_PW): neo4j_password,
        'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY):
            extracted_search_data_path,
        'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY): 'w',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_PATH_CONFIG_KEY):
            extracted_search_data_path,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_MODE_CONFIG_KEY): 'r',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY):
            elasticsearch_client,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY):
            elasticsearch_new_index_key,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY):
            elasticsearch_doc_type_key,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY):
            elasticsearch_index_alias,
    })

    # only optionally add these keys, so need to dynamically `put` them
    if cypher_query:
        job_config.put('extractor.search_data.{}'.format(Neo4jSearchDataExtractor.CYPHER_QUERY_CONFIG_KEY),
                       cypher_query)
    if elasticsearch_mapping:
        job_config.put('publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_MAPPING_CONFIG_KEY),
                       elasticsearch_mapping)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=ElasticsearchPublisher())
    return job


if __name__ == "__main__":
    # Uncomment next line to get INFO level logging
    # logging.basicConfig(level=logging.INFO)

    if connection_string():
        sql = textwrap.dedent("""
            select user_email AS email, split_part(user_name_erp, ' ',1) AS first_name, split_part(user_name_erp, ' ',2) AS last_name, user_name_erp AS name
            from dwh.user_dm;
            """)

        # start user job
        job_user = create_sample_job(sql, 'user_dm',
                                     'databuilder.models.user.User')
        job_user.launch()

        user_cypher_query = textwrap.dedent(
            """
            MATCH (user:User)
            OPTIONAL MATCH (user)-[read:READ]->(a)
            OPTIONAL MATCH (user)-[own:OWNER_OF]->(b)
            OPTIONAL MATCH (user)-[follow:FOLLOWED_BY]->(c)
            OPTIONAL MATCH (user)-[manage_by:MANAGE_BY]->(manager)
            with user, a, b, c, read, own, follow, manager
            where user.full_name is not null
            return user.email as email, user.first_name as first_name, user.last_name as last_name,
            user.full_name as name, user.github_username as github_username, user.team_name as team_name,
            user.employee_type as employee_type, manager.email as manager_email, user.slack_id as slack_id,
            user.is_active as is_active,
            REDUCE(sum_r = 0, r in COLLECT(DISTINCT read)| sum_r + r.read_count) AS total_read,
            count(distinct b) as total_own,
            count(distinct c) AS total_follow
            order by user.email
            """
        )

        user_elasticsearch_mapping = """
            {
              "mappings":{
                "user":{
                  "properties": {
                    "email": {
                      "type":"text",
                      "analyzer": "simple",
                      "fields": {
                        "raw": {
                          "type": "keyword"
                        }
                      }
                    },
                    "first_name": {
                      "type":"text",
                      "analyzer": "simple",
                      "fields": {
                        "raw": {
                          "type": "keyword"
                        }
                      }
                    },
                    "last_name": {
                      "type":"text",
                      "analyzer": "simple",
                      "fields": {
                        "raw": {
                          "type": "keyword"
                        }
                      }
                    },
                    "name": {
                      "type":"text",
                      "analyzer": "simple",
                      "fields": {
                        "raw": {
                          "type": "keyword"
                        }
                      }
                    },
                    "total_read":{
                      "type": "long"
                    },
                    "total_own": {
                      "type": "long"
                    },
                    "total_follow": {
                      "type": "long"
                    }
                  }
                }
              }
            }
        """

        job_es_user = create_es_publisher_sample_job(
            elasticsearch_index_alias='user_search_index',
            elasticsearch_doc_type_key='user',
            model_name='databuilder.models.user_elasticsearch_document.UserESDocument',
            cypher_query=user_cypher_query,
            elasticsearch_mapping=user_elasticsearch_mapping)
        job_es_user.launch()
