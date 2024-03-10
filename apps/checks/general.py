from apps.checks.analytics.general import check_analytics, check_analytics_telegram
from bins.general.enums import Chain, Protocol


def check_all():

    check_analytics()


def main(option: str, **kwargs):
    if option == "prices":
        raise NotImplementedError("Prices check not implemented")
    if option == "analytics":
        check_analytics()
    if option == "hypervisor_status":
        raise NotImplementedError("Hypervisor status check not implemented")
    if option == "all":
        check_all()
    if option == "queue":
        raise NotImplementedError("Queue check not implemented")
    if option == "reward_status":
        raise NotImplementedError("Reward status check not implemented")
    if option == "operations":
        raise NotImplementedError("operations check not implemented")
    if option == "analytics_telegram":
        check_analytics_telegram()


# check analytics
# check last operation
# check last static update
# chekc last hype status timestamp
# chekc last revenue operation
# check last reward status timestamp
# check last hypervisor returns timestamp
# check latest multifeedistributor timestamps
# check queue status ( quantify the number of jobs in queue, stuck jobs, etc):
#           prices: check how old
#           operations should not be here: check if address is actually a hypervisor ( sometimes its not)
#           latest_multi_feedistributor: check count=5 are really stuck ( how?)
#           hypervisor_status: should not be here. check and control.

# check latest prices timestamps
