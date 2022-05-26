import logging
import os

from typing import Dict, Optional

from amundsen_application.base.base_redash_preview_client import BaseRedashPreviewClient


LOGGER = logging.getLogger(__name__)
DEFAULT_URL = 'https://redashstage.sparedev.com'
# mapping database.cluster to a Redash query ID
SOURCE_DB_QUERY_MAP = {
    'postgres.analytics': 46,
    'athena.AwsDataCatalog': 45
}
REDASH_USER_API_KEY = os.environ.get('REDASH_USER_API_KEY', 'Fi8eNMwexImx5rGjAhCRABVb4cbPZmDvUl1SLvlY')


class StorableRedashPreviewClient(BaseRedashPreviewClient):
    def __init__(self,
                 *,
                 redash_host: str = DEFAULT_URL,
                 user_api_key: Optional[str] = REDASH_USER_API_KEY) -> None:
        super().__init__(redash_host=redash_host, user_api_key=user_api_key)
        self.default_query_limit = 50
        self.max_redash_cache_age = 0  # run new query every time

    def get_redash_query_id(self, params: Dict) -> Optional[int]:
        database = params['database']
        cluster = params['cluster']
        db_cluster_key = f'{database}.{cluster}'
        return SOURCE_DB_QUERY_MAP.get(db_cluster_key)

    def _get_query_api_key(self, params: Dict) -> Optional[str]:
        return REDASH_USER_API_KEY

    def build_redash_query_params(self, params: Dict) -> Dict:
        """
        Builds a dictionary of parameters that will be injected into the Redash query
        template. The keys in this dictionary MUST be a case-sensitive match to the
        template names in the Redash query and you MUST have the exact same parameters,
        no more, no less.

        Override this function to provide custom values.
        """
        return {
            'parameters': {
                'schema_name': params.get('schema'),
                'table_name': params.get('tableName'),
            },
            'max_age': self.max_redash_cache_age
        }