import re

from airflow import AirflowException

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property
from typing import Any, Optional, Dict
from uuid import uuid4

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.hooks.athena import AWSAthenaHook


class AthenaOperatorExtended(AWSAthenaOperator):
    """
        A wrapper class that adds a new functionalities for AWSAthenaOperator:

        - Support for multiple SQL commands in a file separated by semicolon

        Sample usage:
        ```
        params_athena = {
            "database": "athena_database",
            "output_location": f"s3://bucket/athena-query-results/",
            "workgroup": "DataScienceWorkgroup"
        }

        refresh_athena_table = AthenaOperatorExtended(
            task_id="refresh_athena_table",
            query="CREATE TABLE {{params.full_table_name}} (id STRING);"
            "MSCK REPAIR TABLE {{ params.full_table_name }}",
            params={
                "full_table_name": "athena_table",
            },
            aws_conn_id=aws_conn,
            **params_athena
        )
        ```
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.query = None
        self.client_request_token = None

    def execute(self, context: dict):
        self.query_execution_context['Database'] = self.database
        self.result_configuration['OutputLocation'] = self.output_location

        scripts = re.split(r";$|;\n", self.query)

        if len(scripts) < 1:
            raise AirflowException("Invalid SQL")

        query_ids = []

        for script in scripts:
            if len(script.strip()) > 0:
                self.log.info(f"Running:\n{script}")
                self.client_request_token = str(uuid4())
                self.query = script

                execution_id = super().execute(context)

                query_ids.append(execution_id)

        return query_ids


class AthenaSQLCheckOperator(BaseOperator):
    """
        This functions the same as ``airflow.operators.sql.SQLCheckOperator``
        but for AWS Athena. This class performs checks against an AWS Athena table. This class expects a sql query that will return a single row
        with numeric or boolean values.

        Sample usage:
        ```
        params_athena = {
            "database": "athena_database",
            "output_location": f"s3://bucket/athena-query-results/",
            "workgroup": "DataScienceWorkgroup"
        }

        check_recency_athena = AthenaSQLCheckOperator(
            task_id="check_recency_athena",
            sql="SELECT COUNT(*) "
                "FROM {{ params.full_table_name }} "
                "WHERE event_date = DATE'{{data_interval_end.in_tz('Asia/Manila').date()}}'",
            params={
                "full_table_name": "athena_table"
            },
            aws_conn_id=aws_conn,
            **params_athena
        )
        ```

    """
    template_fields = ("sql", "database", "output_location")

    def __init__(self,
                 aws_conn_id: str,
                 sql: str,
                 database: str,
                 output_location: str,
                 client_request_token: Optional[str] = None,
                 workgroup: str = "primary",
                 query_execution_context: Optional[Dict[str, str]] = None,
                 result_configuration: Optional[Dict[str, Any]] = None,
                 sleep_time: int = 30,
                 **kwargs):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.sql = sql
        self.database = database
        self.output_location = output_location
        self.client_request_token = client_request_token or str(uuid4())
        self.workgroup = workgroup
        self.query_execution_context = query_execution_context or {}
        self.result_configuration = result_configuration or {}
        self.sleep_time = sleep_time
        self.query_execution_id = None
        self.max_tries = None

    @cached_property
    def hook(self) -> AWSAthenaHook:
        """Create and return an AWSAthenaHook."""
        return AWSAthenaHook(self.aws_conn_id, sleep_time=self.sleep_time)

    def execute(self, context: Any):
        self.query_execution_context['Database'] = self.database
        self.result_configuration['OutputLocation'] = self.output_location

        self.log.info(f"Running: \n{self.sql}")

        self.query_execution_id = self.hook.run_query(
            self.sql,
            self.query_execution_context,
            self.result_configuration,
            self.client_request_token,
            self.workgroup,
        )

        query_status = self.hook.poll_query_status(self.query_execution_id, self.max_tries)

        if query_status in AWSAthenaHook.FAILURE_STATES:
            error_message = self.hook.get_state_change_reason(self.query_execution_id)
            raise Exception(
                f'Final state of Athena job is {query_status}, query_execution_id is '
                f'{self.query_execution_id}. Error: {error_message}'
            )
        elif not query_status or query_status in AWSAthenaHook.INTERMEDIATE_STATES:
            raise Exception(
                f'Final state of Athena job is {query_status}. Max tries of poll status exceeded, '
                f'query_execution_id is {self.query_execution_id}.'
            )

        results = self.hook.get_query_results(self.query_execution_id)
        try:
            result = _get_first_row(results)
        except IndexError:
            raise AirflowException(
                f"Invalid results, possibly empty. "
                f"query_execution_id is {self.query_execution_id}"
            )
        except TypeError:
            raise AirflowException(
                f"Invalid result. SQL query should return a valid numeric data."
                f"query_execution_id is {self.query_execution_id}"
            )

        if not result:
            raise AirflowException("The query returned None")
        elif not all(bool(r) for r in result):
            raise AirflowException("Test failed.")
        elif all(bool(r) for r in result):
            self.log.info("Success")

        AirflowException("Something went wrong")

    def on_kill(self) -> None:
        """Cancel the submitted athena query"""
        if self.query_execution_id:
            self.log.info('Received a kill signal.')
            self.log.info('Stopping Query with executionId - %s', self.query_execution_id)
            response = self.hook.stop_query(self.query_execution_id)
            http_status_code = None
            try:
                http_status_code = response['ResponseMetadata']['HTTPStatusCode']
            except Exception as ex:
                self.log.error('Exception while cancelling query: %s', ex)
            finally:
                if http_status_code is None or http_status_code != 200:
                    self.log.error('Unable to request query cancel on athena. Exiting')
                else:
                    self.log.info(
                        'Polling Athena for query with id %s to reach final state', self.query_execution_id
                    )
                    self.hook.poll_query_status(self.query_execution_id)


def _get_first_row(result):
    data = result["ResultSet"]["Rows"][1]["Data"][0]
    output = []
    for val in data.values():
        if val.isnumeric():
            val = float(val)
        elif val.lower() in ["true"]:
            val = True
        elif val.lower() in ["false"]:
            val = False
        else:
            raise Exception(f"Invalid query result: {val}")

        output.append(val)

    return output
