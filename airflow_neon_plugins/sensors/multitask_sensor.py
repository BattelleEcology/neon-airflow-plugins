#!/usr/bin/env python
""" Multi Task Sensor Operator code to be used in DAG """
from airflow import settings
from airflow.sensors.base import BaseSensorOperator
from airflow.models import TaskInstance, SkipMixin
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State


class MultiExternalTaskSensor(BaseSensorOperator, SkipMixin):
    """
    Checks the same task ID in multiple dags
    :param external_dag_list: A list of dags to check for the task
    :type external_dag_tasks: list
    :param external_task_id: The external task id to check
    :type external_task_id: string
    :param allowed_states: list of allowed states, default is ``['success']``
    :type allowed_states: list
    :param execution_delta: time difference with the previous execution to
        look at, the default is the same execution_date as the current task.
        For yesterday, use [positive!] datetime.timedelta(days=1). Either
        execution_delta or execution_date_fn can be passed to
        ExternalTaskSensor, but not both.
    :type execution_delta: datetime.timedelta
    :param execution_date_fn: function that receives the current execution date
        and returns the desired execution dates to query. Either execution_delta
        or execution_date_fn can be passed to ExternalTaskSensor, but not both.
    :type execution_date_fn: callable
    """

    ui_color = "#19647e"

    @apply_defaults
    def __init__(
        self,
        external_dag_list,
        external_task_id,
        allowed_states=None,
        execution_delta=None,
        execution_date_fn=None,
        *args,
        **kwargs
    ):
        super(MultiExternalTaskSensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]
        if execution_delta is not None and execution_date_fn is not None:
            raise ValueError(
                "Only one of `execution_date` or `execution_date_fn` may"
                "be provided to MultiExternalTaskSensor; not both."
            )

        self.execution_delta = execution_delta
        self.execution_date_fn = execution_date_fn
        self.external_dag_list = external_dag_list
        self.external_task_id = external_task_id

    def poke(self, context):
        # This is from the regular sensor
        if self.execution_delta:
            dttm = context["execution_date"] - self.execution_delta
        elif self.execution_date_fn:
            dttm = self.execution_date_fn(context["execution_date"])
        else:
            dttm = context["execution_date"]

        dttm_filter = dttm if isinstance(dttm, list) else [dttm]
        serialized_dttm_filter = ",".join(
            [datetime.isoformat() for datetime in dttm_filter]
        )

        self.log.info(
            "Poking for "
            "{self.external_dag_list}."
            "{self.external_task_id} on "
            "{} ... ".format(serialized_dttm_filter, **locals())
        )
        num_dags = len(self.external_dag_list)
        TI = TaskInstance
        session = settings.Session()
        count = (
            session.query(TI)
            .filter(
                TI.dag_id.in_(self.external_dag_list),
                TI.task_id == self.external_task_id,
                TI.state.in_(self.allowed_states),
                TI.execution_date.in_(dttm_filter),
            )
            .count()
        )
        session.commit()
        session.close()
        # Does the count match the number of dags?
        if num_dags == count:
            return count
        else:
            self.log.info(
                "Only {} of {} tasks matched on {}...".format(
                    count, num_dags, serialized_dttm_filter
                )
            )
            return 0
