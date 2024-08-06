import pytest

from clx.utils.fun_utils import arg


def test_arg_positional():
    assert arg(0, "name", ["value"], {}) == "value"
    assert arg(1, "name", ["value1", "value2"], {}) == "value2"
    assert arg(0, "name", [None], {}) is None
    with pytest.raises(ValueError):
        arg(2, "name", ["value1", "value2"], {})


def test_arg_keyword():
    assert arg(0, "name", [], {"name": "value"}) == "value"
    assert arg(0, "name", [], {"name": None}) is None


def test_arg_both():
    assert arg(0, "name", ["value"], {"name": "value"}) == "value"
    assert arg(1, "name", ["value1", "value2"], {"name": "value2"}) == "value2"
    with pytest.raises(ValueError):
        arg(0, "name", ["value1"], {"name": "value2"})


def test_arg_none():
    with pytest.raises(ValueError):
        arg(0, "name", [], {})
