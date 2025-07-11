import logging
import tomllib
from collections import Counter
from pathlib import Path

import flatdict

logger = logging.getLogger(__name__)

GLOBAL_CONFIG_REQUIRED_KEYS = set(["client_n_workers", "client_threads_per_worker"])
CDR_CONFIG_REQUIRED_KEYS = set(["description"])


class CDRTransformerFileManager:
    def __init__(self, config_file: Path, input_path: Path, output_path: Path):
        self.config_file = Path(config_file)
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.setup()

    def setup(self):
        if not all(
            [
                self.config_file.exists(),
                self.input_path.exists(),
            ]
        ):
            logger.error(
                f"One or more of the `{self.config_file}`, `{self.input_path}` paths do not exist. Please provide valid paths."
            )
            raise FileNotFoundError(
                f"One or more of the `{self.config_file}`, `{self.input_path}` paths do not exist. Please provide valid paths."
            )
        
        if not self.output_path.exists():
            self.output_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created output path: {self.output_path}")


        self.config = CDRTransformerFileManager.load_config(self.config_file)
        self.providers_paths = CDRTransformerFileManager.get_providers_paths(
            self.input_path, self.config
        )
        if self.providers_paths:
            logger.info("Providers paths found:")
            for provider_path in self.providers_paths:
                path = provider_path["path"]
                file_count = f"{provider_path['file_count']:,d}".replace(",", ".")
                file_suffix = provider_path["file_suffix"]
                logger.info(
                    f"  {path}, found {file_count} files with suffix {file_suffix}."
                )

    @staticmethod
    def load_config(config_file: Path):
        """Read and parse the configuration file"""

        if not config_file.exists():
            logger.error(f"Configuration file {config_file} does not exist.")
            raise FileNotFoundError(f"Configuration file {config_file} does not exist.")

        with open(config_file, "rb") as f:
            config = tomllib.load(f)

        flatconfig = flatdict.FlatDict(config)

        global_sections = set(
            [
                key.rsplit(":", 1)[0]
                for key in flatconfig.keys()
                if key.startswith("global:")
            ]
        )
        cdr_sections = set(
            [
                key.rsplit(":", 1)[0]
                for key in flatconfig.keys()
                if key.startswith("cdr:")
            ]
        )

        for section in global_sections:
            if not GLOBAL_CONFIG_REQUIRED_KEYS.issubset(flatconfig.get(section).keys()):
                logger.error(
                    f"Config file is missing a required key in section `{section}`"
                )
                raise KeyError(
                    f"Config file is missing a required key in section `{section}`"
                )

        for section in cdr_sections:
            if not CDR_CONFIG_REQUIRED_KEYS.issubset(flatconfig.get(section).keys()):
                logger.error(
                    f"Config file is missing a required key in section `{section}`"
                )
                raise KeyError(
                    f"Config file is missing a required key in section `{section}`"
                )

        return flatconfig

    @staticmethod
    def get_leaf_directories(root_path):
        """
        Get all leaf directories (directories with no subdirectories) in a directory tree.

        Parameters
        ----------
        root_dir : str or Path
            The root directory path to search from.

        Returns
        -------
        list of Path
            List of Path objects representing all leaf directories.
        """
        root_path = Path(root_path)

        if not root_path.exists() or not root_path.is_dir():
            logger.error(f"Path does not exist or is not a directory: {root_path}")
            raise FileNotFoundError(
                f"Path does not exist or is not a directory: {root_path}"
            )

        leaf_directories = []

        # Walk through all directories in the tree
        for path in root_path.rglob("*"):
            if path.is_dir():
                # Check if this directory has any subdirectories
                has_subdirs = any(child.is_dir() for child in path.iterdir())
                if not has_subdirs:
                    leaf_directories.append(path)
        if len(leaf_directories) >= 1:
            return sorted(leaf_directories)
        else:
            return [root_path]

    @staticmethod
    def get_providers_paths(input_path, config):
        input_path = Path(input_path)

        if not (input_path.exists() and input_path.is_dir()):
            logger.error(
                f"Input directory {input_path} does not exist or is not a directory."
            )
            raise FileNotFoundError(
                f"Input directory {input_path} does not exist or is not a directory."
            )

        providers_paths = []
        if cdr_config := config.get("cdr", {}):
            if leaf_directories := CDRTransformerFileManager.get_leaf_directories(
                input_path
            ):
                for path in leaf_directories:
                    vendor = path.name
                    provider = path.parent.name
                    provider_key = f"{provider}:{vendor}"
                    if path_config := cdr_config.get(
                        provider_key, cdr_config.get(vendor, None)
                    ):
                        files = list(path.glob("*.*"))
                        files_suffix_counter = Counter([file.suffix for file in files])
                        if len(files) == 0:
                            logger.info(f"No files found in {path}. Skipping.")
                            continue
                        elif len(files_suffix_counter) == 1:
                            file_type, file_count = files_suffix_counter.most_common()[
                                0
                            ]
                        else:
                            logger.error(
                                f"Multiple file types found in {path}. Please ensure only one type of file is present."
                            )
                            raise TypeError(
                                f"Multiple file types found in {path}. Please ensure only one type of file is present."
                            )
                        providers_paths.append(
                            {
                                "path": path,
                                "provider": provider,
                                "vendor": vendor,
                                "config": dict(path_config),
                                "file_suffix": file_type,
                                "file_count": file_count,
                                # "files": files,
                            }
                        )

        return providers_paths
