from dstools.pipeline import DAG
from dstools.pipeline.products import File
from dstools.pipeline.tasks import DownloadFromURL


def test_can_download_file(tmp_directory):
    dag = DAG()

    url = """
    https://archive.ics.uci.edu/ml/machine-learning-databases/iris/{{product}}
    """
    DownloadFromURL(url, File('iris.data'), dag=dag)

    assert dag.build()
