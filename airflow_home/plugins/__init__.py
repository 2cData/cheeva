from airflow.plugins_manager import AirflowPlugin

import airflow_home.plugins.helpers as helpers
import airflow_home.plugins.operators as operators


class MetadataManagement(AirflowPlugin):
    name = "perficient_data_management_plugin",
    operators = [
                    operators.HasRowsOperator,
                    operators.DataQualityOperator,
                ],
    helpers = [
        helpers.RunSQL,
        helpers.CREATE_DUMMY_TABLE_SQL,
    ]
