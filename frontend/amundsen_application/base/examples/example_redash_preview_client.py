# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
import os

from typing import Dict, Optional

from amundsen_application.base.base_redash_preview_client import BaseRedashPreviewClient


LOGGER = logging.getLogger(__name__)


# Redash natively runs on port 5000, the same port as Amundsen.
# Make sure to update the running port to match your deployment!
DEFAULT_URL = 'https://redashstage.sparedev.com/'


# Update this mapping with your database.cluster and Redash query ID
SOURCE_DB_QUERY_MAP = {
    'postgres.analytics': 46,
    'athena.AwsDataCatalog': 45
}

# This example uses a common, system user, for the API key
REDASH_USER_API_KEY = 'Fi8eNMwexImx5rGjAhCRABVb4cbPZmDvUl1SLvlY'


def _build_db_cluster_key(params: Dict) -> str:
    _db = params.get('database')
    _cluster = params.get('cluster')

    db_cluster_key = f'{_db}.{_cluster}'
    return db_cluster_key


class RedashSimplePreviewClient(BaseRedashPreviewClient):
    def __init__(self,
                 *,
                 redash_host: str = DEFAULT_URL,
                 user_api_key: Optional[str] = REDASH_USER_API_KEY) -> None:
        super().__init__(redash_host=redash_host, user_api_key=user_api_key)

    def get_redash_query_id(self, params: Dict) -> Optional[int]:
        """
        Retrieves the query template that should be executed for the given
        source / database / schema / table combination.

        Redash Connections are generally unique to the source and database.
        For example, Snowflake account that has two databases would require two
        separate connections in Redash. This would require at least one query
        template per connection.

        The query ID can be found in the URL of the query when using the Redash GUI.
        """
        db_cluster_key = _build_db_cluster_key(params)
        return SOURCE_DB_QUERY_MAP.get(db_cluster_key)


class RedashComplexPreviewClient(BaseRedashPreviewClient):
    def __init__(self,
                 *,
                 redash_host: str = DEFAULT_URL,
                 user_api_key: Optional[str] = REDASH_USER_API_KEY) -> None:
        super().__init__(redash_host=redash_host, user_api_key=user_api_key)
        self.default_query_limit = 100
        self.max_redash_cache_age = 3600  # One Hour

    def _get_query_api_key(self, params: Dict) -> Optional[str]:
        return REDASH_USER_API_KEY

    def get_redash_query_id(self, params: Dict) -> Optional[int]:
        db_cluster_key = _build_db_cluster_key(params)
        return SOURCE_DB_QUERY_MAP.get(db_cluster_key)
