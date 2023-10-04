def check_list_for_value(item: list, values: list, previous_keys: list = []) -> list:
    result = []
    for v in item:
        if isinstance(v, dict):
            if sub_result := check_dict_for_value(
                item=v, values=values, previous_keys=previous_keys
            ):
                result.append(sub_result)
        elif hasattr(v, "__iter__") and not isinstance(v, str):
            if sub_result := check_list_for_value(
                item=v, values=values, previous_keys=previous_keys
            ):
                result.append(sub_result)
        else:
            if v in values:
                result.append(previous_keys)

    return result


def check_dict_for_value(item: dict, values: list, previous_keys: list = []) -> list:
    result = []
    for k, v in item.items():
        if isinstance(v, dict):
            if sub_result := check_dict_for_value(
                item=v, values=values, previous_keys=previous_keys + [k]
            ):
                result.append(sub_result)
        elif hasattr(v, "__iter__") and not isinstance(v, str):
            if sub_result := check_list_for_value(
                item=v, values=values, previous_keys=previous_keys + [k]
            ):
                result.append(sub_result)
        else:
            if v in values:
                result.append(previous_keys + [k])

    return result
