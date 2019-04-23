from pathlib import Path

from dstools.pipeline.dag import DAG
from dstools.pipeline.tasks import Task
from dstools.pipeline.products import File


def test_non_existent_file():
    f = File('file.txt')
    assert not f.exists()
    assert f.outdated()
    assert f.outdated_code_dependency()
    assert not f.outdated_data_dependencies()


def test_outdated_data_simple_dependency(tmp_directory):
    """ A -> B
    """
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')

    ta = Task('echo hi', File(fa), dag)
    tb = Task('echo hi', File(fb), dag)

    tb.set_upstream(ta)

    assert not tb.product.exists()
    assert tb.product.outdated()
    assert tb.product.outdated_code_dependency()
    assert tb.product.outdated_data_dependencies()

    fa.touch()
    fb.touch()

    assert tb.product.exists()

    # FIXME: provide a method for refreshing metadata
    tb.product.metadata = tb.product.fetch_metadata()


def test_outdated_code_simple_dependency():
    """ A -> B
    """
    pass


def test_outdated_data_and_code_simple_dependency():
    """ A -> B
    """
    pass


def test_many_upstream():
    """ {A, B} -> C
    """
    pass


def test_many_downstream():
    """ A -> {B, C}
    """
    pass


def test_chained_dependency():
    """ A -> B -> C
    """
    pass
