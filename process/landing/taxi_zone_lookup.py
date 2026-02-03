import json
import logging
import os
import sys
from datetime import datetime, timedelta

import requests
from env_vars import data_root_path

# LOG ######################################################
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

logger = logging.getLogger(__name__)
# LOG ######################################################


# GET ARGS
_PARAMS = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}
logger.info(f"\n _PARAMS: {_PARAMS} \n")

start_time = datetime.now()

########################################################################
# READ DATA ############################################################

topic = "taxi_zone_lookup"
landing_path = f"{data_root_path}/landing/{topic}"

if not os.path.exists(landing_path):
    os.makedirs(landing_path)

###########################################################################
# PROCESS DATA ############################################################

file_name = f"taxi_zone_lookup.csv"

logger.info(f"getting data")

try:
    _URL = f"https://d37ci6vzurychx.cloudfront.net/misc/{file_name}"
    resp = requests.get(_URL)

except Exception as err:
    logger.error(f"Erro on getting data")
    raise err

# #######################################################################
# WRITE DATA ############################################################
# save clean data

logger.info(f"writing data on datalake")

try:
    full_path_file = os.path.join(landing_path, file_name)

    file = open(full_path_file, "wb")
    file.write(resp.content)
    file.close()

except Exception as err:
    logger.error(f"Erro on write data on datalake")
    raise err

logger.info(f"total time process: {datetime.now() - start_time}")