import logging
import shutil
import traceback
import uuid

import dask.dataframe as dd

from teletransformer.config import (
    ETL_PARTITION_SIZE,

)

logger = logging.getLogger(__name__)


def extract_parquet(provider_path, input_files, working_path):
    logger.error("Not implemented extractor")
    raise ValueError("Not implemented extractor")


def extract_text(provider_path, input_files, working_path):
    logger.error("Not implemented extractor")
    raise ValueError("Not implemented extractor")

