from pathlib import Path

from dstools.pipeline.dag import DAG
from dstools.pipeline.tasks import NotebookRunner
from dstools.pipeline.products import File


def test_can_execute_ipynb(path_to_assets, tmp_directory):
    dag = DAG()

    NotebookRunner(path_to_assets / 'sample.ipynb',
                   product=File(Path(tmp_directory, 'out.ipynb')),
                   dag=dag)
    dag.build()
