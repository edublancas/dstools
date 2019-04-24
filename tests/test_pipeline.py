from pathlib import Path

from dstools.pipeline.dag import DAG
from dstools.pipeline.tasks import Task, BashCommand
from dstools.pipeline.products import File


def test_non_existent_file():
    dag = DAG()
    f = File('file.txt')
    ta = Task('echo hi', f, dag)

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

    ta = BashCommand('touch a.txt', File(fa), dag)
    tb = BashCommand('touch b.txt', File(fb), dag)

    tb.set_upstream(ta)

    assert not ta.product.exists()
    assert not tb.product.exists()
    assert ta.product.outdated()
    assert tb.product.outdated()

    dag.build()

    # they both exist now
    assert ta.product.exists()
    assert tb.product.exists()

    # and arent outdated...
    assert not ta.product.outdated()
    assert not tb.product.outdated()

    # let's make b outdated
    ta.build(force=True)

    assert not ta.product.outdated()
    assert tb.product.outdated()


def test_many_upstream():
    """ {A, B} -> C
    """
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    ta = BashCommand('touch a.txt', File(fa), dag)
    tb = BashCommand('touch b.txt', File(fb), dag)
    tc = BashCommand('touch c.txt', File(fc), dag)

    tc.set_upstream(ta)
    tc.set_upstream(tb)

    dag.build()

    assert ta.product.exists()
    assert tb.product.exists()
    assert tc.product.exists()

    assert not ta.product.outdated()
    assert not tb.product.outdated()
    assert not tc.product.outdated()

    ta.build(force=True)

    assert not ta.product.outdated()
    assert not tb.product.outdated()
    assert tc.product.outdated()

    dag.build()
    tb.build(force=True)

    assert not ta.product.outdated()
    assert not tb.product.outdated()
    assert tc.product.outdated()


def test_many_downstream():
    """ A -> {B, C}
    """
    pass


def test_chained_dependency():
    """ A -> B -> C
    """
    pass



def test_can_create_task_with_many_products():
    dag = DAG()
    fa1 = File('a1.txt')
    fa2 = File('a2.txt')
    ta = Task('echo hi', [fa1, fa2], dag)

    assert not ta.product.exists()
    assert ta.product.outdated()
    assert ta.product.outdated_code_dependency()
    assert not ta.product.outdated_data_dependencies()
