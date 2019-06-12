from connect.utils import normalize_names, take_first_from_lists


def test_normalize_names():
    a = "hello"
    b = "los-angels"
    assert "hello.losangels" == normalize_names(a, b)


def test_first_from_list():
    a = {"a": [1], "b": [2, 3, 4, 5]}
    b = {"a": 1, "b": [2, 3, 4, 5]}
    assert b == take_first_from_lists(a)
