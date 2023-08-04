# Al
# {"jsonrpc":"2.0","id": 1, "method": "eth_subscribe", "params": ["logs", {"address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "topics": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]}]}

import logging
from bins.general.net_utilities import WebsocketClient


class topic_hook(WebsocketClient):
    def __init__(self, url: str, subscription_params: dict):
        super().__init__(url, subscription_params)

    def on_open(self):
        logging.getLogger().debug(f" Socket subscribed at {self.url}")

    def on_close(self):
        logging.getLogger().debug(f" Socket closed from {self.url}")

    def on_message(self, msg):
        logging.getLogger().debug(f" Socket message received from {self.url}")

    def on_error(self, e, data=None):
        self.error = e
        self.stop = True
        logging.getLogger().error(
            " Socket error encountered. stopping client.  error: {}   data: {}".format(
                e, data
            )
        )
