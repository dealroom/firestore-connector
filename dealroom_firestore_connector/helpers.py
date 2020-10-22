import logging
import traceback


def error_logger(message, error_code=0):
    """Logs formatted error messages on the stderr file."""
    logging.error(
        "Error trace: %s\n[Error code %d] %s"
        % (traceback.format_exc(), error_code, message)
    )
