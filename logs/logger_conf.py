import logging
import os
import sys
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

logs_dir = os.path.join(os.path.dirname(__file__))
log_file_path = os.path.join(logs_dir, f"{__name__}{time.time()}.log")

handler = logging.FileHandler(log_file_path, mode='a')
handler2 = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(handler2)
