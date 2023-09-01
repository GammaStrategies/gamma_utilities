def add_to_memory(key, value):
    """Add to temporal memory a key and value"""
    if key not in CONFIGURATION["_custom_"]["temporal_memory"]:
        CONFIGURATION["_custom_"]["temporal_memory"][key] = []

    if value not in CONFIGURATION["_custom_"]["temporal_memory"][key]:
        CONFIGURATION["_custom_"]["temporal_memory"][key].append(value)


def get_from_memory(key) -> list:
    """Get value from temporal memory"""
    try:
        return CONFIGURATION["_custom_"]["temporal_memory"][key]
    except KeyError:
        return []
