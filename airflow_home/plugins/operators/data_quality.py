import logging

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Perform data quality checks using sql queries
    """

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 table="",
                 query="",
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self._conn_id = conn_id
        self._table = table
        self._query = query

    def execute(self, context):
        self.log.info(f'Checking DataQuality for {self._table}')
        snowflake_hook = SnowflakeHook(self._conn_id)
        self.log.info(f'Running query: {self._query}')
        records = snowflake_hook.get_records(self._query)
        if len(records) < 1 or len(records[0] < 1):
            raise ValueError(f"Data quality check failed. {self._table} returned no results")
        logging.info(f'Passed Data Quality Check for {self._table}')
