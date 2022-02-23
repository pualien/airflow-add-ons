from airflow.operators.python import PythonOperator


class TemplatedPythonOperator(PythonOperator):
    template_fields = ('templates_dict', 'op_args', 'op_kwargs')
