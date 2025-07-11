import logging
import shutil
import traceback
import uuid

import dask.dataframe as dd

from teletransformer.config import (
    ERICSSON_TEXT_COLUMNS_TO_READ,
    ETL_PARTITION_SIZE,
    SCHEMA,
)

logger = logging.getLogger(__name__)


def extract_parquet(provider_path, input_files, working_path):
    logger.error("Not implemented extractor")
    raise ValueError("Not implemented extractor")


def extract_text(provider_path, input_files, working_path):
    cdr_config = provider_path.get("config", {})
    delimiter = cdr_config.get("delimiter", ";")
    encoding = cdr_config.get("encoding", "utf-8")
    npartitions = cdr_config.get("npartitions", 0)

    try:
        columns_to_keep = list(ERICSSON_TEXT_COLUMNS_TO_READ.keys())
        extracted_ddf = dd.read_csv(
            input_files,
            sep=delimiter,
            usecols=columns_to_keep,
            dtype=str,
            encoding=encoding,
        )
        extracted_ddf = extracted_ddf[columns_to_keep]
        extracted_ddf.columns = extracted_ddf.columns.map(ERICSSON_TEXT_COLUMNS_TO_READ)

        extracted_ddf["DT_CHAMADA"] = dd.to_datetime(
            extracted_ddf["DT_CHAMADA"], format="%Y-%m-%d", errors="coerce"
        )
        extracted_ddf["DT_CHAMADA"] = extracted_ddf["DT_CHAMADA"].dt.date
        extracted_ddf["HORA_CHAMADA"] = dd.to_datetime(
            extracted_ddf["HORA_CHAMADA"], format="%H:%M:%S", errors="coerce"
        )
        extracted_ddf["HORA_CHAMADA"] = extracted_ddf["HORA_CHAMADA"].dt.time

        extracted_ddf["QT_TEMPO_CONVERSACAO"] = (
            extracted_ddf["QT_TEMPO_CONVERSACAO"].fillna("0").astype(int)
        )

        extracted_file = working_path / str(uuid.uuid4())
        logger.info(f"Saving extracted files to {extracted_file}.")
        if extracted_file.exists():
            logger.info(f"Extracted file {extracted_file} alredy exists deleting it...")
            shutil.rmtree(extracted_file)

        # extract and save to file
        if npartitions > 0:
            extracted_ddf.repartition(npartitions=npartitions).to_parquet(
                extracted_file, schema=SCHEMA
            )
        else:
            extracted_ddf.repartition(partition_size=ETL_PARTITION_SIZE).to_parquet(
                extracted_file, schema=SCHEMA
            )

        assert extracted_file.exists()

        return extracted_file

    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Failed to process {input_files.parent}: {e}\n{error_details}")
        raise
