#!/usr/bin/env python
""" Executing sql operator that can be used for an XCOM """

from __future__ import absolute_import, division, print_function

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SQLExecuteOperator(BaseOperator):
    """
    Executes a sql query against a connection. Expects a sql query that returns a single row
    :param conn_id: reference to a DB connection
    :type conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param field_list: list of names for returned fields
    :type list: ['row1', 'row2', 'row3']
    :returns a dict if a field_list is passed otherwise a list
    """

    template_fields = ('sql',)
    template_ext = ('.hql', '.sql',)
    ui_color = '#fff7e6'

    @apply_defaults
    def __init__(self, sql, conn_id=None, parameters=None, field_list=None,
                 *args, **kwargs):
        super(SQLExecuteOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql
        self.parameters = parameters
        self.field_list = field_list

    def execute(self, context=None):
        self.log.info("Executing: %s", self.sql)
        records = self.get_db_hook().get_first(self.sql)
        self.log.info('Record: %s', records)
        # Keep in mind xcom returns are pickled
        if self.field_list is None:
            return records
        else:
            return dict(zip(self.field_list, records))

    def get_db_hook(self):
        return BaseHook.get_hook(conn_id=self.conn_id)
