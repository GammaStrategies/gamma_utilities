def check_list_for_value(item: list, values: list, previous_keys: list = []) -> list:
    result = []
    for v in item:
        if isinstance(v, dict):
            check_dict_for_value(item=v, values=values, previous_keys=previous_keys)
        elif hasattr(v, "__iter__") and not isinstance(v, str):
            check_list_for_value(item=v, values=values, previous_keys=previous_keys)
        else:
            if v in values:
                result.append(previous_keys)

    return result


def check_dict_for_value(item: dict, values: list, previous_keys: list = []) -> list:
    result = []
    for k, v in item.items():
        if isinstance(v, dict):
            check_dict_for_value(
                item=v, value=values, previous_keys=previous_keys + [k]
            )
        elif hasattr(v, "__iter__") and not isinstance(v, str):
            check_list_for_value(
                item=v, value=values, previous_keys=previous_keys + [k]
            )
        else:
            if v in values:
                result.append(previous_keys + [k])

    return result
