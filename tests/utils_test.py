from utils import search_in_nested_dict, validate_input


def test_search_in_nested_dict():
    test_dict = {"hello": "there"}
    key = "hello"
    value = search_in_nested_dict(test_dict, key)
    assert value == "there"

    test_dict = {"hello": {"this is": "a test"}}
    key = "this is"
    value = search_in_nested_dict(test_dict, key)
    assert value == "a test"

    key = "bye"
    value = search_in_nested_dict(test_dict, key)
    assert not value


def test_validate_input():
    record = "wrong"
    try:
        validate_input(record)
    except TypeError:
        assert True

    record = {"a": "b"}
    try:
        validate_input(record)
    except KeyError:
        assert True

    record = {
    "change": [
      {
        "kind": "insert",
        "schema": "public",
        "columnnames": [],
        "columntypes": [],
        "columnvalues": []
      }
    ]
    }
    try:
        validate_input(record)
    except KeyError:
        assert True

    record = {
        "change": [
            {
                "kind": "insert",
                "table": "test",
                "schema": "public",
                "columnnames": ['a'],
                "columntypes": ['b'],
                "columnvalues": ['c']
            }
        ]
    }
    try:
        validate_input(record)
        assert True
    except KeyError:
        assert False
