import logging
import shutil
import traceback

import dask.dataframe as dd
import pyarrow as pa
from dask.distributed import Client, LocalCluster

import teletransformer.extractors.ericsson as ericsson
import teletransformer.extractors.nokia as nokia

# from teletransformer.preprocessing import extract_normalized_numbers

logger = logging.getLogger(__name__)


DATE_COLUMN = "DT_CHAMADA"
DATE_FORMAT = "%Y-%m-%d"
TIME_COLUMN = "HORA_CHAMADA"
TIME_FORMAT = "%H:%M:%S"
CHARGEABLE_DURATION_COLUMN = "QT_TEMPO_CONVERSACAO"

EXTRACTORS = {
    "ericsson": ericsson,
    "nokia": nokia,
    "tim:volte": None,
    "vivo:volte": None,
}


class CDRTransformer:
    def __init__(self, client_config={}):
        """
        Initialize the DaskTransformer with a specified number of workers.

        :param n_workers: Number of Dask workers to use.
        """

        self.cluster = LocalCluster(
            n_workers=client_config.get("client_n_workers",12),
            threads_per_worker=client_config.get("client_threads_per_worker",1),
            dashboard_address=client_config.get("client_dashboard_address",":8888"),
        )
        self.client = Client(self.cluster)

    
    @staticmethod
    def _extract_cdr(provider_path, working_path):
        
        provider = provider_path.get("provider")
        vendor = provider_path.get("vendor")
        extractor_key = f"{provider}:{vendor}"
        
        if extractor := EXTRACTORS.get(extractor_key,EXTRACTORS.get(vendor,None)):
            input_path = provider_path.get("path")
            file_suffix = provider_path.get("file_suffix")
            input_files = input_path / f"*{file_suffix}"   
            if file_suffix == '.parquet':
                return extractor.extract_parquet(provider_path, input_files, working_path)
            else:
                return extractor.extract_text(provider_path, input_files, working_path)
        else:
            logger.error(f"Unknown or not implemented extractor: {provider}:{vendor}")
            raise ValueError(f"Unknown or not implemented extractor: {provider}:{vendor}")
        
    @staticmethod
    def transform(provider_path, output_path, preprocess=False):
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

        output_file = output_path / f"{provider}/{vendor}/{provider}_{vendor}.parquet"
        working_path = output_path / ".working"

        logger.info(f"Started reading {input_files} files...")
        extracted_file = CDRTransformer._extract_cdr(provider_path, input_files, working_path)

        logger.info(f"Input files saved sucessfuly to {extracted_file}")

        # reload data from parquet file
        extracted_ddf = dd.read_parquet(extracted_file)

        try:
            extracted_ddf[DATE_COLUMN] = dd.to_datetime(
                extracted_ddf[DATE_COLUMN], format=DATE_FORMAT, errors="coerce"
            ).dt.date
            extracted_ddf[TIME_COLUMN] = dd.to_datetime(
                extracted_ddf[TIME_COLUMN], format=TIME_FORMAT, errors="coerce"
            ).dt.time
        except Exception as e:
            error_details = traceback.format_exc()
            logger.error(
                f"Failed to convert date and time columns: {e}\n{error_details}"
            )
            raise

        extracted_ddf = extracted_ddf.fillna("-1")
        extracted_ddf[CHARGEABLE_DURATION_COLUMN] = extracted_ddf[
            CHARGEABLE_DURATION_COLUMN
        ].astype(int)

        # if preprocess:
        #     logger.info("Cleaning numbers columns...")
        #     clean_numbers_columns = ["na_clean", "na_valid", "nb_clean", "nb_valid"]
        #     clean_numbers_meta = {
        #         "na_clean": str,
        #         "na_valid": bool,
        #         "nb_clean": str,
        #         "nb_valid": bool,
        #     }
        #     extracted_ddf[clean_numbers_columns] = extracted_ddf.apply(
        #         lambda row: pd.Series(
        #             extract_normalized_numbers(row["number_a"], row["number_b"]),
        #             index=clean_numbers_columns,
        #         ),
        #         axis=1,
        #         meta=clean_numbers_meta,
        #     )
        # logger.info("Cleaning numbers option was not set skipping...")

        if output_file.exists():
            logger.info(f"Output file {output_file} alredy exists deleting it...")
            shutil.rmtree(output_file)

        logger.info(f"Saving transformed files to {output_file}.")
        schema = {DATE_COLUMN: pa.date32(), TIME_COLUMN: pa.time64("us")}
        extracted_ddf.to_parquet(output_file, schema=schema)

        # if extracted_file.exists():
        #     logger.info(f"Cleaning working file {extracted_file}")
        #     shutil.rmtree(extracted_file, ignore_errors=True)

        logger.info(f"Transformation completed for {provider}:{vendor}.")
