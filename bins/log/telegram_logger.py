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
        t = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        return "<i>{datetime}</i><pre>\n{message}</pre>".format(
            message=record.msg, datetime=t
        )
        # <b>bold</b>, <strong>bold</strong>
        # <i>italic</i>, <em>italic</em>
        # <a href="http://www.example.com/">inline URL</a>
        # <a href="tg://user?id=123456789">inline mention of a user</a>
        # <code>inline fixed-width code</code>
        # <pre>pre-formatted fixed-width code block</pre>


class send_to_telegram:

    @staticmethod
    def info(
        msg: str | list[str] | None = None, topic: str | None = None, dtime: bool = True
    ) -> requests.Response | None:
        """Send info message thru telegram

        Args:
            message (str):
            dtime (bool, optional): . Defaults to True.

        Returns:
            requests.Response | None:
        """
        if msg is None:
            return
        message_lines = [msg] if isinstance(msg, str) else msg

        # add header to message
        header_message = ""
        if dtime:
            header_message = f"<i>{datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}</i>"
        # add topic to header
        if topic:
            header_message += f"<b> :INFO: </b><i> {topic}</i>"

        # send message
        return send_to_telegram.send_telegram_html(header_message, message_lines)

    @staticmethod
    def warning(
        msg: str | list[str] | None = None, topic: str | None = None, dtime: bool = True
    ) -> requests.Response | None:
        """Send warning message thru telegram

        Args:
            message (str):
            dtime (bool, optional): . Defaults to True.

        Returns:
            requests.Response | None:
        """
        if msg is None:
            return
        message_lines = [msg] if isinstance(msg, str) else msg

        # add header to message
        header_message = ""
        if dtime:
            header_message = f"<i>{datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}</i>"
        # add topic to header
        if topic:
            header_message += f"<b> :WARNING: </b><i> {topic}</i>"
        # send message
        return send_to_telegram.send_telegram_html(header_message, message_lines)

    @staticmethod
    def error(
        msg: str | list[str] | None = None, topic: str | None = None, dtime: bool = True
    ) -> requests.Response | None:
        """Send error message thru telegram

        Args:
            message (str):
            dtime (bool, optional): . Defaults to True.

        Returns:
            requests.Response | None:
        """
        if msg is None:
            return
        message_lines = [msg] if isinstance(msg, str) else msg

        # add header to message
        header_message = ""
        if dtime:
            header_message = f"<i>{datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}</i>"
        # add topic to header
        if topic:
            header_message += f"<b> :ERROR: </b><i> {topic}</i>"
        # send message
        return send_to_telegram.send_telegram_html(header_message, message_lines)

    @staticmethod
    def send_telegram_html(
        header: str, message_lines: list[str]
    ) -> requests.Response | None:
        """Manually send html message thru telegram

        Args:
            html_message (str):
            dtime (bool, optional): . Defaults to True.

        Returns:
            requests.Response | None:
        """
        # include utc header time
        if not TELEGRAM_ENABLED:
            logging.getLogger(__name__).debug(
                "Telegram is not enabled. Can't send message"
            )
            return

        # add message lines
        message = header + "\n" + "\n".join(message_lines)

        # create payload
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
        }
        if TELEGRAM_TOKEN != "" and TELEGRAM_CHAT_ID != "":
            return requests.post(
                "https://api.telegram.org/bot{token}/sendMessage".format(
                    token=TELEGRAM_TOKEN
                ),
                data=payload,
            ).content


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
