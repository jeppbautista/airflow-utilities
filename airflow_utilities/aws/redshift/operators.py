import re

from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator


class RedshiftOperatorExtended(RedshiftSQLOperator):
    """
    A wrapper class that adds a new functionalities for RedshiftSQLOperator:

    - Support for multiple SQL commands in a file separated by semicolon

    Sample usage:
    ```
    upsert_table = RedshiftOperatorExtended(
        task_id="upsert_table",
        sql='''
        DELETE FROM {{params.full_table_name}}
            WHERE event_date = DATE'{{data_interval_end.in_tz('Asia/Manila').date()}}';

        INSERT INTO {{params.full_table_name}}
        SELECT * FROM source_table;
        ''',
        params={
            "full_table_name": redshift_table
        }
    )
    ```

    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context: dict):
        scripts = re.split(r";$|;\n", self.sql)

        if len(scripts) < 1:
            raise Exception("Invalid SQL")

        for script in scripts:
            if len(script.strip()) > 0:
                self.sql = script
                super().execute(context)

        return True