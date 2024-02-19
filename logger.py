# First start logging
import logging
from logging.handlers import RotatingFileHandler

from config import LOG_DIR, LOG_FILE_NAME, LOG_MAX_BYTES, LOG_FILE_NUMBERS

# Setup logging
# Set logging parameters
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
fh = RotatingFileHandler('{}{}'.format(LOG_DIR, LOG_FILE_NAME), mode='a', maxBytes=LOG_MAX_BYTES, backupCount=LOG_FILE_NUMBERS, encoding=None, delay=0)
ch.setLevel(logging.INFO)
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s %(name)s - %(process)s - %(funcName)s(%(lineno)d) - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(ch)
logger.addHandler(fh)

