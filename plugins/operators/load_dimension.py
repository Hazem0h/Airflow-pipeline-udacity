from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    @apply_defaults
    def __init__(self,
                 table= "",
                 redshift_conn_id = "",
                 table_sepcific_sql = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.table_sepcific_sql = table_sepcific_sql

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        postgres_hook.run(
            SqlQueries.redshift_sql_insert.format(
                self.table,
                self.table_sepcific_sql
            )
        )
