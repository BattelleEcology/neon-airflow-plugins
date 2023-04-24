#!/usr/bin/env python
""" Presto Operator code to be used in DAG """

from __future__ import absolute_import, division, print_function
# Non-bundled operator plugin for presto
from airflow.hooks.presto_hook import PrestoHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class PrestoOperator(BaseOperator):
    """
    Executes sql code in a specific Presto cluster
    :param presto_conn_id: reference to a specific Presto connection
    :type presto_conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self, sql, presto_conn_id='presto_default', parameters=None,
                 *args, **kwargs):
        super(PrestoOperator, self).__init__(*args, **kwargs)
        self.presto_conn_id = presto_conn_id
        self.sql = sql
        self.parameters = parameters

    def execute(self, context):
        self.log.info("Executing: %s", self.sql)
        hook = PrestoHook(presto_conn_id=self.presto_conn_id)
        # For Presto, fetchall needs to be called even on non-result-returning queries
        # See https://github.com/dropbox/PyHive/issues/138
        hook.get_records(
            self.sql,
            parameters=self.parameters)
