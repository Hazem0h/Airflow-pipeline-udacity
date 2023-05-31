from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    dim_sql_insert = """
        INSERT INTO {}
        {}
        ;
    """
    truncate_table_sql = """
        TRUNCATE TABLE {};
    """
    @apply_defaults
    def __init__(self,
                 table= "",
                 redshift_conn_id = "",
                 table_sepcific_sql = "",
                 truncate_table = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.table_sepcific_sql = table_sepcific_sql
        self.truncate_table = truncate_table

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        sql = LoadDimensionOperator.dim_sql_insert
        if self.truncate_table:
            self.log.info("Old data in the table will be truncated")
            truncate_table_sql = LoadDimensionOperator.truncate_table_sql.format(self.table)
            sql = truncate_table_sql + '\n' + sql
        
        self.log.info("Inserting data into the dimension table")
        postgres_hook.run(
            sql.format(
                self.table,
                self.table_sepcific_sql
            )
        )
        self.log.info("Dimensional data inserted")
