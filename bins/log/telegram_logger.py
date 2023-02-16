import requests
from logging import Handler, Formatter
import logging
import datetime

TELEGRAM_ENABLED = True
TELEGRAM_TOKEN = ""
TELEGRAM_CHAT_ID = ""


class RequestsHandler(Handler):
    def emit(self, record):
        if TELEGRAM_ENABLED == True:
            log_entry = self.format(record)
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": log_entry,
                "parse_mode": "HTML",
            }
            if TELEGRAM_TOKEN != "" and TELEGRAM_CHAT_ID != "":
                return requests.post(
                    "https://api.telegram.org/bot{token}/sendMessage".format(
                        token=TELEGRAM_TOKEN
                    ),
                    data=payload,
                ).content


class LogstashFormatter(Formatter):
    def __init__(self):
        super(LogstashFormatter, self).__init__()

    def format(self, record):
        t = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        return "<i>{datetime}</i><pre>\n{message}</pre>".format(
            message=record.msg, datetime=t
        )
        # <b>bold</b>, <strong>bold</strong>
        # <i>italic</i>, <em>italic</em>
        # <a href="http://www.example.com/">inline URL</a>
        # <a href="tg://user?id=123456789">inline mention of a user</a>
        # <code>inline fixed-width code</code>
        # <pre>pre-formatted fixed-width code block</pre>


# TEST
def test():

    logger = logging.getLogger("trymeApp")
    logger.setLevel(logging.WARNING)

    handler = RequestsHandler()
    formatter = LogstashFormatter()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.setLevel(logging.WARNING)

    logger.error("We have a problem")


if __name__ == "__main__":
    test()
