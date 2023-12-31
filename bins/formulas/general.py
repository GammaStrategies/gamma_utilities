import statistics


def itentify_valid_and_outliers(
    items: list,
) -> tuple[list[tuple[int, object]], list[tuple[int, object]]]:
    """Identify outliers in a list of numbers using the Normal Distribution and Standard Deviation

    Args:
        items (list): list of numbers

    Returns:
        tuple[list[tuple[int, object]],list[tuple[int, object]]]:
                <valid_result>: list of tuples with the <items> index and the value of the valid items,
                <invalid_result>: list of tuples with the <items> index and the value of the outliers
    """
    mean = statistics.mean(items)
    sd = statistics.stdev(items)

    result_valid = []
    result_invalid = []

    for idx, x in enumerate(items):
        if x > mean - 2 * sd and x < mean + 2 * sd:
            # good values
            result_valid.append((idx, x))
        else:
            # outliers
            result_invalid.append((idx, x))

    return result_valid, result_invalid
