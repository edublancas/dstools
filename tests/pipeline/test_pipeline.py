import subprocess
from pathlib import Path

from dstools.pipeline.dag import DAG
from dstools.pipeline.tasks import BashCommand, BashCommand
from dstools.pipeline.products import File

kwargs = {'stderr': subprocess.PIPE,
          'stdout': subprocess.PIPE,
          'shell': True}


def test_non_existent_file():
    dag = DAG()
    f = File('file.txt')
    ta = BashCommand('echo hi > {{product}}', f, dag, 'ta')
    ta.render()

    assert not f.exists()
    assert f._outdated()
    assert f._outdated_code_dependency()
    assert not f._outdated_data_dependencies()


def test_outdated_data_simple_dependency(tmp_directory):
    """ A -> B
    """
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')

    ta = BashCommand('touch {{product}}', File(fa), dag, 'ta', {}, kwargs,
                     False)
    tb = BashCommand('cat {{upstream["ta"]}} > {{product}}', File(fb), dag,
                     'tb', {}, kwargs, False)

    ta >> tb

    ta.render()
    tb.render()

    assert not ta.product.exists()
    assert not tb.product.exists()
    assert ta.product._outdated()
    assert tb.product._outdated()

    dag.build()

    # they both exist now
    assert ta.product.exists()
    assert tb.product.exists()

    # and arent outdated...
    assert not ta.product._outdated()
    assert not tb.product._outdated()

    # let's make b outdated
    ta.build(force=True)

    assert not ta.product._outdated()
    assert tb.product._outdated()


def test_many_upstream(tmp_directory):
    """ {A, B} -> C
    """
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    ta = BashCommand('touch {{product}}', File(fa),
                     dag, 'ta', {}, kwargs, False)
    tb = BashCommand('touch {{product}} > {{product}}', File(fb),
                     dag, 'tb', {}, kwargs, False)
    tc = BashCommand('cat {{upstream["ta"]}} {{upstream["tb"]}} >  {{product}}',
                     File(fc), dag, 'tc', {}, kwargs, False)

    (ta + tb) >> tc

    dag.build()

    assert ta.product.exists()
    assert tb.product.exists()
    assert tc.product.exists()

    assert not ta.product._outdated()
    assert not tb.product._outdated()
    assert not tc.product._outdated()

    ta.build(force=True)

    assert not ta.product._outdated()
    assert not tb.product._outdated()
    assert tc.product._outdated()

    dag.build()
    tb.build(force=True)

    assert not ta.product._outdated()
    assert not tb.product._outdated()
    assert tc.product._outdated()


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
    ta = BashCommand('echo {{product}}', [fa1, fa2], dag, 'ta')
    ta.render()

    assert not ta.product.exists()
    assert ta.product._outdated()
    assert ta.product._outdated_code_dependency()
    assert not ta.product._outdated_data_dependencies()


def test_overloaded_operators():
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    ta = BashCommand('touch {{product}}', File(fa), dag, 'ta')
    tb = BashCommand('touch {{product}}', File(fb), dag, 'tb')
    tc = BashCommand('touch {{product}}', File(fc), dag, 'tc')

    ta >> tb >> tc

    assert not ta.upstream
    assert tb in tc.upstream.values()
    assert ta in tb.upstream.values()


def test_adding_tasks():
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    ta = BashCommand('touch {{product}}', File(fa), dag, 'ta')
    tb = BashCommand('touch {{product}}', File(fb), dag, 'tb')
    tc = BashCommand('touch {{product}}', File(fc), dag, 'tc')

    assert list((ta + tb).tasks) == [ta, tb]
    assert list((tb + ta).tasks) == [tb, ta]
    assert list((ta + tb + tc).tasks) == [ta, tb, tc]
    assert list(((ta + tb) + tc).tasks) == [ta, tb, tc]
    assert list((ta + (tb + tc)).tasks) == [ta, tb, tc]


def test_adding_tasks_left():
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    ta = BashCommand('touch {{product}}', File(fa), dag, 'ta')
    tb = BashCommand('touch {{product}}', File(fb), dag, 'tb')
    tc = BashCommand('touch {{product}}', File(fc), dag, 'tc')

    (ta + tb) >> tc

    assert not ta.upstream
    assert not tb.upstream
    assert list(tc.upstream.values()) == [ta, tb]


def test_adding_tasks_right():
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    ta = BashCommand('touch {{product}}', File(fa), dag, 'ta')
    tb = BashCommand('touch {{product}}', File(fb), dag, 'tb')
    tc = BashCommand('touch {{product}}', File(fc), dag, 'tc')

    ta >> (tb + tc)

    assert not ta.upstream
    assert list(tb.upstream.values()) == [ta]
    assert list(tc.upstream.values()) == [ta]
