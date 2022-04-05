def search_in_nested_dict(jsonb_value, column_name):
    """Recursively search for values of column_name in JSON.
       Returns the value of the given key if exists else None.
    """
    if isinstance(jsonb_value, dict):
        for key, value in jsonb_value.items():
            if isinstance(value, (dict, list)):
                return search_in_nested_dict(value, column_name)
            elif key == column_name:
                return value


def validate_input(record):
    """Checks if the record has the expected format and raises an error if not.
    """
    if type(record) is not dict:
        raise TypeError
    if not record.get("change") or type(record.get("change")) is not list \
            or len(record.get("change")) != 1 or not record["change"][0].get("table") \
            or record["change"][0].get("kind") != "insert":
        raise KeyError
    change = record["change"][0]
    if not change.get("columnnames") or not change.get("columnvalues") or \
            not change.get("columntypes"):
        raise KeyError
    if len(change.get("columnnames")) != len(change.get("columnvalues")) or len(change.get("columnvalues")) != len(
            change.get("columntypes")):
        raise KeyError
    if type(change) is not dict or type(change["columnnames"]) is not list \
            or type(change["columnvalues"]) is not list or \
            type(change["columntypes"]) is not list:
        raise TypeError
