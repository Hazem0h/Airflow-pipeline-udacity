from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    fact_insert_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 table_specific_sql = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.table_specific_sql = table_specific_sql

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Inserting Data to Fact Table")
        postgres_hook.run(
            LoadFactOperator.fact_insert_sql.format(
                self.table,
                self.table_specific_sql
            )
        )
        self.log.info("Data Insertion completed")


