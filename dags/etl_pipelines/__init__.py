"""ETL Pipelines модуль для Airflow DAGs."""

from etl_pipelines.extract import pluginsdb, gitlab
from etl_pipelines.transform import scripts, projectsync, logs, gitlab as gitlab_transform
from etl_pipelines.load import datalake

__all__ = [
    'pluginsdb',
    'gitlab',
    'scripts',
    'projectsync',
    'logs',
    'gitlab_transform',
    'datalake'
]
