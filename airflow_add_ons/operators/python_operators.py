try:
    from airflow.operators.python import PythonOperator
except Exception:
    from airflow.operators.python_operator import PythonOperator


class TemplatedPythonOperator(PythonOperator):
    template_fields = ('templates_dict', 'op_args', 'op_kwargs')
