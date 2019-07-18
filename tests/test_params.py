from dstools.pipeline.tasks.Params import Params


def test_can_get_first():
    p = Params()

    p['a'] = 0
    p['b'] = 1

    assert p.first == 0
