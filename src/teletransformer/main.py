import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

from teletransformer.filemanager import CDRTransformerFileManager
from teletransformer.transformer import CDRTransformer

# default values for development purposes only
DEFAULT_CONFIG_FILE = Path(__file__).parents[2] / "tests/teletransformer.toml"
DEFAULT_SOURCE_PATH = "/data/cdr/cdr_processado/Amostra"
DEFAULT_OUTPUT_PATH = "/data/cdr/cdr_processado/Amostra/transformado"


# Initialize a placeholder logger - will be properly configured later
logger = logging.getLogger("teletransformer")


# Configure logging
def setup_logging(output_path: Path, log_level: int = logging.INFO):
    """Set up logging to both file and console"""
    # Create logs directory if it doesn't exist
    logs_dir = output_path / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Create a timestamped log file name
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"teletransformer_{timestamp}.log"
    # keep log in a single file. for development purposes only
    log_file = logs_dir / "teletransformer.log"

    # Configure root logger
    logger = logging.getLogger("teletransformer")
    logger.setLevel(log_level)

    # Clear any existing handlers
    if logger.handlers:
        logger.handlers.clear()

    # File handler - detailed format with timestamps
    file_handler = logging.FileHandler(log_file)
    file_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(file_format)
    file_handler.setLevel(log_level)

    # # Console handler - using Rich for better formatting
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Capture warnings to the logging system
    logging.captureWarnings(True)

    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger


def main():
    parser = argparse.ArgumentParser(
        description="Handle arguments for teletransformer."
    )
    parser.add_argument(
        "-C",
        "--config_file",
        type=str,
        default=DEFAULT_CONFIG_FILE,
        help="Path to the configuration file.",
    )
    parser.add_argument(
        "-I",
        "--input_path",
        type=str,
        default=DEFAULT_SOURCE_PATH,
        help="Path to the input directory, where parsed CDR files are stored.",
    )
    parser.add_argument(
        "-O",
        "--output_path",
        type=str,
        default=DEFAULT_OUTPUT_PATH,
        help="Path to the output directory, where transformed CDR files will be stored.",
    )
    args = parser.parse_args()

    if not all([args.config_file, args.input_path, args.output_path]):
        parser.error(
            "All arguments -C, -I, and -O are required. Please provide valid paths."
        )
    else:
        config_file = Path(args.config_file)
        input_path = Path(args.input_path)
        output_path = Path(args.output_path)

    config_file = Path(config_file)
    input_path = Path(input_path)
    output_path = Path(output_path)

    # Set up logging
    global logger
    logger = setup_logging(output_path, logging.INFO)

    logger.info("Starting teletransformer with arguments:")
    logger.info(f"  config_file: {config_file}")
    logger.info(f"  input_path: {input_path}")
    logger.info(f"  output_path: {output_path}")

    manager = CDRTransformerFileManager(
        config_file=config_file, input_path=input_path, output_path=output_path
    )
    client_config = manager.config.get("global", {})
    transformer = CDRTransformer(client_config)
    for provider_path in manager.providers_paths:
        transformer.transform(provider_path, output_path)


if __name__ == "__main__":
    main()
