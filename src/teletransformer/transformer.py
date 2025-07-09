import logging
import shutil
import traceback
import uuid
import pyarrow as pa
import json
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client, LocalCluster

from teletransformer.preprocessing import extract_normalized_numbers

logger = logging.getLogger(__name__)

ETL_PARTITION_SIZE = "50MB"  # Default partition size for Dask ETL operations
DATE_TIME_COLUMNS = set(["date", "time"])
DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H:%M:%S"
DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class DaskTransformer:
    def __init__(self, n_workers=12, threads_per_worker=1, dashboard_address=":8888"):
        """
        Initialize the DaskTransformer with a specified number of workers.

        :param n_workers: Number of Dask workers to use.
        """
        self.cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            dashboard_address=dashboard_address,
        )
        self.client = Client(self.cluster)

    @staticmethod
    def _validate_provider_path_config(provider_path):
        """
        Validate the configuration file."""

        if file_suffix := provider_path.get("file_suffix", None):
            if provider_path_config := provider_path.get("config", {}):
                if file_suffix == ".parquet": # required keys for parquet files
                    try: 
                        record_columns = provider_path_config.get(
                            "record_columns", None
                        )
                        record_columns = json.loads(record_columns)
                    except Exception as e:
                        error_details = traceback.format_exc()
                        logger.error(
                            f"Failed to parse `record_columns` from config file: {e}\n{error_details}"
                        )
                        raise
                    return provider_path_config

                else: # check required keys for text files
                    delimiter = provider_path_config.get("delimiter", None)
                    usecols = provider_path_config.get("usecols", None)
                    names = provider_path_config.get("names", None)
                    date_time_columns = provider_path_config.get(
                        "date_time_columns", None
                    )

                    if not all([delimiter, usecols, names, date_time_columns]):
                        logger.error(
                            "Missing required configuration keys for DaskTransformer. Please check the configuration file."
                        )
                        raise ValueError(
                            "Missing required configuration keys for DaskTransformer. Please check the configuration file."
                        )

                    try:
                        assert len(usecols) == len(names)
                    except AssertionError:
                        logger.error(
                            "The number of columns in usecols and names must be the same."
                        )
                        raise ValueError(
                            "The number of columns in usecols and names must be the same."
                        )
                    return provider_path_config
        return None

    @staticmethod
    def transform(provider_path, output_path, clean_numbers=False):
        """
        Transform the input DataFrame using Dask.

        :param df: Input DataFrame to be transformed
        """

        provider = provider_path.get("provider")
        vendor = provider_path.get("vendor")

        input_path = provider_path.get("path")
        file_suffix = provider_path.get("file_suffix")
        input_files = input_path / f"*{file_suffix}"

        logger.info(f"Started transforming {vendor} files for {provider} ...")
        logger.info(f"  input path: {input_path}")
        logger.info(f"  output path: {output_path}")

        if provider_path_config := DaskTransformer._validate_provider_path_config(provider_path):
            # required keys
            delimiter = provider_path_config.get("delimiter")
            usecols = provider_path_config.get("usecols")
            names = provider_path_config.get("names")
            date_time_columns = provider_path_config.get("date_time_columns")

            # optional keys
            skiprows = provider_path_config.get("skiprows", 0)
            encoding = provider_path_config.get("encoding", "utf-8")
            npartitions = provider_path_config.get("npartitions", 0)
            date_format = provider_path_config.get("date_format", DATE_FORMAT)
            time_format = provider_path_config.get("time_format", TIME_FORMAT)
            date_time_format = provider_path_config.get(
                "date_time_format", DATE_TIME_FORMAT
            )
            record_columns = provider_path_config.get("record_columns", None)

            logger.info(
                "All required configuration keys are present. Proceeding with transformation..."
            )
        else:
            logger.error(
                "Missing required configuration keys. Please check the configuration file."
            )
            raise ValueError(
                "Missing required configuration keys. Please check the configuration file."
            )

        output_file = output_path / f"{provider}/{vendor}/{provider}_{vendor}.parquet"
        working_path = output_path / ".working"

        logger.info(f"Started reading {input_files} files...")
        if file_suffix == ".parquet":
            logger.info("Reading parquet files is not implemented yet.")
            raise NotImplementedError("Reading parquet files is not implemented yet.")
        else:
            try:
                extracted_ddf = dd.read_csv(
                    input_files,
                    sep=delimiter,
                    usecols=usecols,
                    names=names,
                    skiprows=skiprows,
                    dtype=str,
                    encoding=encoding,
                )
                if npartitions > 0:
                    extracted_ddf = extracted_ddf.repartition(npartitions=npartitions)
                else:
                    extracted_ddf = extracted_ddf.repartition(
                        partition_size=ETL_PARTITION_SIZE
                    )
            except Exception as e:
                error_details = traceback.format_exc()
                logger.error(f"Failed to process {input_path}: {e}\n{error_details}")
                raise

        extracted_file = working_path / str(uuid.uuid4())
        logger.info(f"Saving extracted files to {extracted_file}.")
        if extracted_file.exists():
            logger.info(f"Extracted file {extracted_file} alredy exists deleting it...")
            shutil.rmtree(extracted_file)
        extracted_ddf.to_parquet(extracted_file)

        # reload data from parquet file
        extracted_ddf = dd.read_parquet(extracted_file)

        if isinstance(date_time_columns, str):
            date_column, time_column = "date", "time"
            extracted_ddf[date_time_columns] = dd.to_datetime(
                extracted_ddf[date_time_columns],
                format=date_time_format,
                errors="coerce",
            )
            extracted_ddf[date_column] = extracted_ddf[date_time_columns].dt.date
            extracted_ddf[time_column] = extracted_ddf[date_time_columns].dt.time
        elif isinstance(date_time_columns, list) and len(date_time_columns) == 2:
            try:
                date_column, time_column = date_time_columns
                extracted_ddf[date_column] = dd.to_datetime(
                    extracted_ddf[date_column], format=date_format, errors="coerce"
                ).dt.date
                extracted_ddf[time_column] = dd.to_datetime(
                    extracted_ddf[time_column], format=time_format, errors="coerce"
                ).dt.time
            except Exception as e:
                error_details = traceback.format_exc()
                logger.error(
                    f"Failed to extract date and time columns: {e}\n{error_details}"
                )
                raise
        else:
            error_message = (
                "Date and time columns must be a string or a list of two strings."
            )
            logger.error(error_message)
            raise ValueError(error_message)

        # extracted_ddf["duration"] = extracted_ddf["duration"].fillna("-1").astype(int)
        # extracted_ddf = extracted_ddf[RECORD_COLUMNS]

        if record_columns:
            try:
                record_columns = json.loads(record_columns)
                date_column = record_columns.get(date_column)
                time_column = record_columns.get(time_column)

                columns_to_keep = list(record_columns.keys())
                columns_to_save = list(record_columns.values())
                extracted_ddf = extracted_ddf[columns_to_keep]
                extracted_ddf.columns = columns_to_save
            except Exception as e:
                error_details = traceback.format_exc()
                logger.error(f"Failed to extract record columns: {e}\n{error_details}")
                raise

        if clean_numbers:
            logger.info("Cleaning numbers columns...")
            clean_numbers_columns = ["na_clean", "na_valid", "nb_clean", "nb_valid"]
            clean_numbers_meta = {
                "na_clean": str,
                "na_valid": bool,
                "nb_clean": str,
                "nb_valid": bool,
            }
            extracted_ddf[clean_numbers_columns] = extracted_ddf.apply(
                lambda row: pd.Series(
                    extract_normalized_numbers(row["number_a"], row["number_b"]),
                    index=clean_numbers_columns,
                ),
                axis=1,
                meta=clean_numbers_meta,
            )
        logger.info("Cleaning numbers option was not set skipping...")

        if output_file.exists():
            logger.info(f"Output file {output_file} alredy exists deleting it...")
            shutil.rmtree(output_file)

        logger.info(f"Saving transformed files to {output_file}.")
        schema = {date_column: pa.date32(), time_column: pa.time64("us")}
        extracted_ddf.to_parquet(output_file, schema=schema)

        # if extracted_file.exists():
        #     logger.info(f"Cleaning working file {extracted_file}")
        #     shutil.rmtree(extracted_file, ignore_errors=True)

        logger.info(f"Transformation completed for {provider} {vendor}.")
