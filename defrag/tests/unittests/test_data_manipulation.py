from defrag.modules.helpers.data_manipulation import compose, find_first, make_xform, make_transducer, base_step


def test_compose():
    def double(x: int) -> int: return x*2
    def plus3(x: int) -> int: return x+3
    func = compose(double, plus3)
    res = func(1)
    assert res == 5


def test_make_transducer():

    def to_str(_step):
        def inner(acc, val):
            return _step(acc, str(val))
        return inner

    def low3(_step):
        def inner(acc, val):
            if val < 3:
                return _step(acc, val)
            return acc
        return inner

    def inc1(_step):
        def inner(acc, val):
            return _step(acc, val+1)
        return inner

    xform = make_xform(to_str, low3, inc1)
    transducer = make_transducer(xform, base_step, [])
    res = transducer([0, 1, 2])
    assert res == ["1", "2"]


def test_find_first():
    l = sorted([1, 2, 5, 10, 3, 8, 4, 7, 5, 9, 2])
    to_insert = 6
    def relation(item, origin): return origin < item
    index = find_first(l, relation, to_insert)
    l.insert(index, 6)
    assert l == [1, 2, 2, 3, 4, 5, 5, 6, 7, 8, 9, 10]
    m = [7]
    n = [5]
    index = find_first(m, relation, to_insert)
    m.insert(index, to_insert)
    assert m == [6, 7]
    index = find_first(m, relation, to_insert)
    n.insert(index, to_insert)
    assert n == [5, 6]
