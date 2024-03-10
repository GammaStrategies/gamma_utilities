from apps.repair.queue.locked import repair_queue_locked_items


def repair_queue():
    # locked items
    repair_queue_locked_items()

    # TODO: replace with non fixed list lenght solution like the "manual_scrape_from_queue" from  tests
    # try process failed items with count > 10
    # repair_queue_failed_items(
    #    count_gte=CONFIGURATION["_custom_"]["cml_parameters"].queue_count or 10
    # )
