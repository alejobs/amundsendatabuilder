import datetime

import boto3

from pyhocon import ConfigFactory, ConfigTree  # noqa: F401
from typing import Iterator, Union, Dict, Any  # noqa: F401

from databuilder.extractor.base_extractor import Extractor
from databuilder.models.table_last_updated import TableLastUpdated
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata


class GlueLastUpdateExtractor(Extractor):
    """
    Extracts last update time from Glue tables
    """

    CLUSTER_KEY = 'cluster'

    DEFAULT_CONFIG = ConfigFactory.from_dict({CLUSTER_KEY: 'gold'})

    def init(self, conf):
        conf = conf.with_fallback(GlueLastUpdateExtractor.DEFAULT_CONFIG)
        self._cluster = '{}'.format(conf.get_string(GlueLastUpdateExtractor.CLUSTER_KEY))
        self._glue = boto3.client('glue')
        self._extract_iter = None  # type: Union[None, Iterator]

    def extract(self):
        # type: () -> Union[TableMetadata, None]
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self):
        # type: () -> str
        return 'extractor.glue_last_update_extractor'

    def _get_extract_iter(self):
        # type: () -> Iterator[TableMetadata]
        """
        It gets all tables and yields TableMetadata
        :return:
        """
        for row in self._get_raw_extract_iter():

            yield TableLastUpdated(table_name=row['Name'],
                                      last_updated_time_epoch=int(row['UpdateTime'].timestamp()),
                                      schema_name=row['DatabaseName'],
                                      db='glue',
                                      cluster=self._cluster)

    def _get_raw_extract_iter(self):
        # type: () -> Iterator[Dict[str, Any]]
        """
        Provides iterator of results row from glue client
        :return:
        """
        tables = self._search_tables()
        return iter(tables)

    def _search_tables(self):
        tables = []
        data = self._glue.search_tables()
        tables += data['TableList']
        while 'NextToken' in data:
            token = data['NextToken']
            data = self._glue.search_tables(NextToken=token)
            tables += data['TableList']
        return tables
