import logging

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class HasRowsOperator(BaseOperator):
    """
    Data quality check to ensure specified table has rows
    """

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 table="",
                 *args, **kwargs):
        super(HasRowsOperator, self).__init__(*args, **kwargs)

        self._conn_id = conn_id
        self._table = table

    def execute(self, context):
        self.log.info(f'Checking HasRows for {self._table}')
        snowflake_hook = SnowflakeHook(self._conn_id)
        self.log.info(f'Running rcord count: {self._table}')
        records = snowflake_hook.get_records(f"SELECT COUNT(1) FROM {self._table}")
        if len(records) < 1 or len(records[0] < 1):
            raise ValueError(f"Data quality check failed. {self._table} returned no results")
        logging.info(f'Passed Has Rows Check for {self._table}')
