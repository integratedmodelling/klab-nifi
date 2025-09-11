import logging

logger = logging.getLogger("klab-nifi-py")
logger.setLevel(logging.INFO) ## Default is INFO

# Check if the logger has any handlers already set up
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)