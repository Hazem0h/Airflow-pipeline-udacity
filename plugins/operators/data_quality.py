from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tests = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        for test_query, true_val in self.tests:
            records = redshift_hook.get_records(test_query)
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"Data quality check failed. {self.table} returned no results")
                raise ValueError(f"Data quality check failed. {self.table} returned no results")
            num_records = records[0][0]
            if num_records != true_val:
                self.log.error(f"Data quality check failed. {self.table} contained {num_records} records, expected result is {true_val}")
                raise ValueError(f"Data quality check failed. {self.table} contained {num_records} records,  expected result is {true_val}")

            