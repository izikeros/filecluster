"""Module for keeping configuration-related code for the filecluster.

- Comprehensive settings class using Pydantic for validation and environment variable support
- ConfigFactory for creating and manipulating configurations
- Centralized enums for better code readability
- Path objects instead of string concatenation for improved path handling
- Improved docstrings with detailed explanations
- Type hints throughout for better IDE support and static analysis
- Field validator to ensure extensions are lowercase
- Backwards compatibility functions for easier transition
- Better error handling with specific error messages
- Default instances for simpler access to common functionality
"""

import os
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import field_validator
from pydantic.dataclasses import dataclass
from pydantic_settings import BaseSettings

from filecluster import logger


class AssignDateToClusterMethod(Enum):
    """Method for selecting a representative date for the cluster.

    RANDOM - coming from a random file in the cluster
    MEDIAN - median datetime from the cluster
    """

    RANDOM = 1
    MEDIAN = 2


class ClusteringMethod(Enum):
    """Method used to decide whether media files are from the same event."""

    TIME_GAP = 1


class CopyMode(Enum):
    """Mode of operation for the finalization of clustering.

    Attributes:
        COPY: make copy of inbox files in the output directory
        MOVE: move inbox files to the proper location in the output directory
        NOP: 'no operation' - do nothing, useful for testing and development
    """

    COPY = 1
    MOVE = 2
    NOP = 3


class Status(Enum):
    """Cluster status."""

    UNKNOWN = 0
    NEW_CLUSTER = 1
    EXISTING_CLUSTER = 2
    DUPLICATE = 3


class FileClusterSettings(BaseSettings):
    """Settings for filecluster application.

    Can be overridden via environment variables with FILECLUSTER_ prefix.
    """

    # Common file settings
    ini_filename: str = ".cluster.ini"
    image_extensions: list[str] = [
        ".jpg",
        ".jpeg",
        ".dng",
        ".cr2",
        ".tif",
        ".tiff",
        ".heic",
    ]
    video_extensions: list[str] = [".mp4", ".3gp", "mov"]

    # Path settings (production)
    INBOX_DIR: str = "inbox"
    OUTBOX_DIR: str = "outbox_clust"

    # Windows paths
    WINDOWS_BASE_PATH: str = "h:\\incomming\\"
    WINDOWS_LIBRARY_PATHS: list[str] = ["h:\\zdjecia\\"]

    # Linux paths
    LINUX_BASE_PATH: str = "/media/root/Foto/incomming/"
    LINUX_LIBRARY_PATHS: list[str] = ["/media/root/Foto/zdjecia/"]

    # Development settings
    DEV_INBOX_DIR: str = "set_1"
    DEV_OUTBOX_DIR: str = "inbox_clust_test"
    DEV_WINDOWS_BASE_PATH: str = "h:\\incomming"
    DEV_WINDOWS_LIBRARY_PATHS: list[str] = [""]
    DEV_LINUX_BASE_PATH: str = "./"
    DEV_LINUX_LIBRARY_PATHS: list[str] = ["zdjecia", "clusters"]

    # Feature flags
    FORCE_DEEP_SCAN: bool = False
    ASSIGN_TO_CLUSTERS_EXISTING_IN_LIBS: bool = False
    SKIP_DUPLICATED_EXISTING_IN_LIBS: bool = False

    # Time settings
    TIME_GRANULARITY_MINUTES: int = 60

    # Default methods
    DEFAULT_ASSIGN_DATE_METHOD: AssignDateToClusterMethod = (
        AssignDateToClusterMethod.MEDIAN
    )
    DEFAULT_CLUSTERING_METHOD: ClusteringMethod = ClusteringMethod.TIME_GAP

    # Columns definitions
    CLUSTER_DF_COLUMNS: list[str] = [
        "cluster_id",
        "start_date",
        "end_date",
        "median",
        "is_continuous",
        "path",
        "target_path",
        "file_count",
        "new_file_count",
    ]

    MEDIA_DF_COLUMNS: list[str] = [
        "file_name",
        "m_date",
        "c_date",
        "exif_date",
        "date",
        "size",
        "hash_value",
        "image",
        "is_image",
        "status",
        "duplicated_to",
        "duplicated_cluster",
    ]

    @field_validator("image_extensions", "video_extensions", mode="after")
    @classmethod
    def lowercase_extensions(cls, extensions: list[str]) -> list[str]:
        """Ensure all extensions are lowercase."""
        return [ext.lower() for ext in extensions]

    class Config:
        env_prefix = "FILECLUSTER_"
        env_file = ".env"


@dataclass
class Config:
    """Configuration for a filecluster application.

    Attributes:
        in_dir_name: Path to input directory containing media files
        out_dir_name: Path to output directory for clustered media
        watch_folders: List of paths to monitor for existing clusters
        image_extensions: List of recognized image file extensions
        video_extensions: List of recognized video file extensions
        time_granularity: Time gap that separates different events
        assign_date_to_clusters_method: Method for determining cluster dates
        clustering_method: Algorithm for clustering media files
        mode: Operation mode (copy, move, or no operation)
        force_deep_scan: Whether to perform deep scanning of files
        assign_to_clusters_existing_in_libs: Whether to use existing clusters
        skip_duplicated_existing_in_libs: Whether to skip duplicated files
    """

    in_dir_name: Path
    out_dir_name: Path
    watch_folders: list[str]
    image_extensions: list[str]
    video_extensions: list[str]
    time_granularity: timedelta
    assign_date_to_clusters_method: AssignDateToClusterMethod
    clustering_method: ClusteringMethod
    mode: CopyMode
    force_deep_scan: bool
    assign_to_clusters_existing_in_libs: bool
    skip_duplicated_existing_in_libs: bool

    def __repr__(self) -> str:
        rep = [f"{p}:\t{self.__getattribute__(p)}" for p in self.__dataclass_fields__]
        return "\n".join(rep)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, key, value)


class ConfigFactory:
    """Factory for creating and manipulating Config objects."""

    def __init__(self, settings: FileClusterSettings | None = None):
        """Initialize with optional settings override.

        Args:
            settings: Custom settings to use (defaults to FileClusterSettings())
        """
        self.settings = settings or FileClusterSettings()

    def get_config(self, is_development_mode: bool = False) -> Config:
        """Get appropriate configuration based on mode.

        Args:
            is_development_mode: Whether to use development configuration

        Returns:
            Appropriate Config object for the specified mode
        """
        return self._create_config(is_development_mode, os.name)

    def _create_config(self, is_development_mode: bool, os_name: str) -> Config:
        """Create configuration for the specified mode and OS.

        Args:
            is_development_mode: Whether to use development configuration
            os_name: Operating system name ('nt' for Windows, other for Linux/Unix)

        Returns:
            Config object for the specified mode and OS
        """
        # Determine base path and library paths based on OS and mode
        if os_name == "nt":  # Windows
            base_path = (
                self.settings.DEV_WINDOWS_BASE_PATH
                if is_development_mode
                else self.settings.WINDOWS_BASE_PATH
            )
            library_paths = (
                self.settings.DEV_WINDOWS_LIBRARY_PATHS
                if is_development_mode
                else self.settings.WINDOWS_LIBRARY_PATHS
            )
        else:  # Linux/Unix
            base_path = (
                self.settings.DEV_LINUX_BASE_PATH
                if is_development_mode
                else self.settings.LINUX_BASE_PATH
            )
            library_paths = (
                self.settings.DEV_LINUX_LIBRARY_PATHS
                if is_development_mode
                else self.settings.LINUX_LIBRARY_PATHS
            )

        # Determine directory names based on mode
        inbox_dir = (
            self.settings.DEV_INBOX_DIR
            if is_development_mode
            else self.settings.INBOX_DIR
        )
        outbox_dir = (
            self.settings.DEV_OUTBOX_DIR
            if is_development_mode
            else self.settings.OUTBOX_DIR
        )

        # Build full paths
        inbox_path = Path(base_path) / inbox_dir
        outbox_path = Path(base_path) / outbox_dir

        # Set operation mode based on development status
        mode = CopyMode.COPY if is_development_mode else CopyMode.MOVE

        # Debug logging in development mode
        if is_development_mode:
            logger.warning("Using development configuration")

        return Config(
            in_dir_name=inbox_path,
            out_dir_name=outbox_path,
            watch_folders=library_paths,
            image_extensions=self.settings.image_extensions,
            video_extensions=self.settings.video_extensions,
            time_granularity=timedelta(minutes=self.settings.TIME_GRANULARITY_MINUTES),
            assign_date_to_clusters_method=self.settings.DEFAULT_ASSIGN_DATE_METHOD,
            clustering_method=self.settings.DEFAULT_CLUSTERING_METHOD,
            mode=mode,
            force_deep_scan=self.settings.FORCE_DEEP_SCAN,
            assign_to_clusters_existing_in_libs=self.settings.ASSIGN_TO_CLUSTERS_EXISTING_IN_LIBS,
            skip_duplicated_existing_in_libs=self.settings.SKIP_DUPLICATED_EXISTING_IN_LIBS,
        )

    def override_from_cli(
        self,
        config: Config,
        inbox_dir: str | None = None,
        output_dir: str | None = None,
        watch_dir_list: list[str] | None = None,
        force_deep_scan: bool | None = None,
        no_operation: bool | None = None,
        copy_mode: bool | None = None,
        drop_duplicates: bool | None = None,
        use_existing_clusters: bool | None = None,
        **kwargs: Any,
    ) -> Config:
        """Override config parameters with CLI arguments.

        Args:
            config: Base configuration to modify
            inbox_dir: Override for input directory
            output_dir: Override for output directory
            watch_dir_list: Override for watched directories
            force_deep_scan: Whether to force deep scanning
            no_operation: Whether to use no-operation mode
            copy_mode: Whether to use copy mode
            drop_duplicates: Whether to skip duplicated files
            use_existing_clusters: Whether to use existing clusters
            **kwargs: Additional overrides

        Returns:
            Updated configuration

        Raises:
            ValueError: When configuration constraints are violated
        """
        # Apply specific CLI overrides
        if inbox_dir is not None:
            config.in_dir_name = Path(inbox_dir)
        if output_dir is not None:
            config.out_dir_name = Path(output_dir)
        if watch_dir_list is not None:
            config.watch_folders = watch_dir_list
        if force_deep_scan is not None:
            config.force_deep_scan = force_deep_scan
        if drop_duplicates is not None:
            config.skip_duplicated_existing_in_libs = drop_duplicates
        if use_existing_clusters is not None:
            config.assign_to_clusters_existing_in_libs = use_existing_clusters

        # Handle operation mode overrides
        if copy_mode:
            config.mode = CopyMode.COPY
        # No-operation takes precedence over copy mode
        if no_operation:
            config.mode = CopyMode.NOP

        # Apply any additional kwargs
        for key, value in kwargs.items():
            if hasattr(config, key) and value is not None:
                setattr(config, key, value)

        # Validate configuration
        if (
            config.skip_duplicated_existing_in_libs
            or config.assign_to_clusters_existing_in_libs
        ) and not config.watch_folders:
            raise ValueError(
                "Watch folders are required when using duplicate detection or existing clusters"
            )

        return config


# Create default instances for simpler API access
default_settings = FileClusterSettings()
default_factory = ConfigFactory(default_settings)


# Backwards compatibility functions
def get_default_config() -> Config:
    """Provide a default production configuration."""
    return default_factory.get_config(is_development_mode=False)


def get_development_config(os_name: str = os.name) -> Config:
    """Provide a default development configuration."""
    return default_factory._create_config(is_development_mode=True, os_name=os_name)


def get_proper_mode_config(is_development_mode: bool) -> Config:
    """Get config for development or production mode."""
    return default_factory.get_config(is_development_mode=is_development_mode)


def override_config_with_cli_params(
    config: Config,
    inbox_dir: str | None = None,
    no_operation: bool | None = None,
    copy_mode: bool | None = None,
    output_dir: str | None = None,
    watch_dir_list: list[str] | None = None,
    force_deep_scan: bool | None = None,
    drop_duplicates: bool | None = None,
    use_existing_clusters: bool | None = None,
) -> Config:
    """Override config with CLI parameters (backwards compatibility)."""
    return default_factory.override_from_cli(
        config,
        inbox_dir=inbox_dir,
        output_dir=output_dir,
        watch_dir_list=watch_dir_list,
        force_deep_scan=force_deep_scan,
        no_operation=no_operation,
        copy_mode=copy_mode,
        drop_duplicates=drop_duplicates,
        use_existing_clusters=use_existing_clusters,
    )
