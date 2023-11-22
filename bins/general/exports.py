import json
import requests
from bins.configuration import CONFIGURATION


def telegram_send_file(
    input_file_content: bytes,
    full_filename: str,
    mime_type: str,
    telegram_file_type: str = "Document",
    caption: str = "",
):
    """_summary_

    Args:
        file (bytes): _description_
        file_ext (str): _description_
        mime_type (str):
                text/csv
                application/json
                text/plain
                image/apng:
                image/avif : AV1 Image File Format (AVIF)
                image/gif: Graphics Interchange Format (GIF)
                image/jpeg: Joint Photographic Expert Group image (JPEG)
                image/png: Portable Network Graphics (PNG)
                image/svg+xml: Scalable Vector Graphics (SVG)
                image/webp: Web Picture format (WEBP)
                audio/wave, audio/wav, audio/x-wav, audio/x-pn-wav
                audio/webm, video/webm, audio/ogg, video/ogg, application/ogg

        file_type (str, optional): _description_. Defaults to "Document".
        caption (str, optional): _description_. Defaults to "".

    """
    # load telegram chatid n token
    TELEGRAM_ENABLED = (
        CONFIGURATION.get("logs", {}).get("telegram", {}).get("enabled", False)
    )
    TELEGRAM_TOKEN = CONFIGURATION.get("logs", {}).get("telegram", {}).get("token", "")
    TELEGRAM_CHAT_ID = (
        CONFIGURATION.get("logs", {}).get("telegram", {}).get("chat_id", "")
    )
    if TELEGRAM_ENABLED:
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "caption": caption,
            # "thumbnail": None,
        }
        files = {
            f"{telegram_file_type.lower()}": (
                full_filename,
                input_file_content,
                mime_type,
            )
        }

        # post to telegram
        return requests.post(
            "https://api.telegram.org/bot{token}/send{filetype}".format(
                token=TELEGRAM_TOKEN,
                filetype=telegram_file_type,
            ),
            data=data,
            files=files,
        )
