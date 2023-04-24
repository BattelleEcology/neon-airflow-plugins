#!/usr/bin/env python
""" Sensor that fails if any dependency is failed, or upstream_failed. Mean to be run with all_done trigger rule. NOTE: NOT NEEDED AFTER AIRFLOW 1.10.3 """
from __future__ import absolute_import, division, print_function

from sqlalchemy import case, func

# from airflow import settings
from airflow.models import BaseOperator, TaskInstance
from airflow.utils.decorators import apply_defaults
from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow.exceptions import AirflowException, AirflowSkipException


class JoinOperator(BaseOperator):
    """
    Checks if any upstream tasks have failed, or upstream_failed
    and immediately throws an exception. Designed to be used with
    trigger_rule all_done for joining branches together
    """

    ui_color = "#AF33FF"  # Purple

    @apply_defaults
    def __init__(self, parameters=None, *args, **kwargs):
        super(JoinOperator, self).__init__(*args, **kwargs)
        self.parameters = parameters

    @provide_session
    def execute(self, context=None, session=None):
        self.log.info("Checking upstream tasks for failures")
        if not self.upstream_list:
            self.log.info("This task had no upstream tasks")
            return

        ti = context.get('task_instance')
        TI = TaskInstance

        # session = settings.Session()
        qry = (
            session
            .query(
                func.coalesce(func.sum(
                    case([(TI.state == State.SUCCESS, 1)], else_=0)), 0),
                func.coalesce(func.sum(
                    case([(TI.state == State.SKIPPED, 1)], else_=0)), 0),
                func.coalesce(func.sum(
                    case([(TI.state == State.FAILED, 1)], else_=0)), 0),
                func.coalesce(func.sum(
                    case([(TI.state == State.UPSTREAM_FAILED, 1)], else_=0)), 0),
                func.count(TI.task_id),
            )
            .filter(
                TI.dag_id == ti.dag_id,
                TI.task_id.in_(ti.task.upstream_task_ids),
                TI.execution_date == ti.execution_date,
                TI.state.in_([
                    State.SUCCESS, State.FAILED,
                    State.UPSTREAM_FAILED, State.SKIPPED]),
            )
        )
        successes, skipped, failed, upstream_failed, done = qry.first()
        session.commit()
        session.close()
        # Fail if any upstreams failed
        if upstream_failed > 0 or failed > 0:
            raise AirflowException("Upstream Failed")
            self.log.info("Upstreams Failed, setting UPSTREAM_FAILED state")
        # Skip if all upstreams skipped
        elif successes == 0 and skipped > 1:
            raise AirflowSkipException("All upstreams skipped")
        # Otherwise we are successfull
        else:
            return
