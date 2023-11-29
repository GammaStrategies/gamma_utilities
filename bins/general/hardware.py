import os
import psutil


def get_memory_usage(percent: bool = False) -> float:
    """
    Get the memory usage in MB or % total

    Returns:
        float: memory usage in MB
    """
    _mem = psutil.virtual_memory()
    # % usage
    if percent:
        return _mem.percent / 100
    # GB
    return _mem.used / 1000000000


def get_cpu_usage() -> float:
    """
    Get the cpu usage in MB or % total
        (last 5 minutes)

    Returns:
        float: cpu usage in MB
    """
    # % usage
    # Getting loadover15 minutes
    load1, load5, load15 = psutil.getloadavg()
    cpu_usage = load5 / os.cpu_count()
    return cpu_usage
    # whithout os
    return psutil.cpu_percent(5 * 60)
