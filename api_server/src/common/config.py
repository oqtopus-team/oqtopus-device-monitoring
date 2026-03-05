import os

from fastapi import Request
from omegaconf import DictConfig, ListConfig, OmegaConf

from common.types.config import AppConfig, LoggingConfig, ServerConfig

DEFAULT_SERVER_TIMEZONE = "UTC"
DEFAULT_SERVER_HOST = "127.0.0.1"
DEFAULT_SERVER_PORT = 8080
DEFAULT_LOCK_TIMEOUT_HOURS = 1
DEFAULT_API_SERVER_CONFIG_PATH = "./config/config.yaml"
DEFAULT_API_SERVER_LOGGING_CONFIG_PATH = "./config/logging.yaml"
DEFAULT_API_SERVER_LOGGING_DIR_PATH = "./logs"
DEFAULT_API_SERVER_OPERATION_HISTORY_PATH = "./data"
DEFAULT_API_SERVER_CONFIG_PATH_INSIDE = "/config/config.yaml"
DEFAULT_API_SERVER_LOGGING_CONFIG_PATH_INSIDE = "/config/logging.yaml"
DEFAULT_API_SERVER_LOGGING_DIR_PATH_INSIDE = "/app/logs"
DEFAULT_API_SERVER_OPERATION_HISTORY_PATH_INSIDE = "/app/data"

ENV_API_SERVER_TIMEZONE = "API_SERVER_TIMEZONE"
ENV_API_SERVER_HOST = "API_SERVER_HOST"
ENV_API_SERVER_PORT = "API_SERVER_PORT"
ENV_VICTORIAMETRICS_URL = "VICTORIAMETRICS_URL"
ENV_API_SERVER_LOCK_TIMEOUT_HOURS = "API_SERVER_LOCK_TIMEOUT_HOURS"
ENV_API_SERVER_CONFIG_PATH = "API_SERVER_CONFIG_PATH"
ENV_API_SERVER_LOGGING_CONFIG_PATH = "API_SERVER_LOGGING_CONFIG_PATH"
ENV_API_SERVER_LOGGING_DIR_PATH = "API_SERVER_LOGGING_DIR_PATH"
ENV_API_SERVER_OPERATION_HISTORY_PATH = "API_SERVER_OPERATION_HISTORY_PATH"
ENV_API_SERVER_CONFIG_PATH_INSIDE = "API_SERVER_CONFIG_PATH_INSIDE"
ENV_API_SERVER_LOGGING_CONFIG_PATH_INSIDE = "API_SERVER_LOGGING_CONFIG_PATH_INSIDE"
ENV_API_SERVER_LOGGING_DIR_PATH_INSIDE = "API_SERVER_LOGGING_DIR_PATH_INSIDE"
ENV_API_SERVER_OPERATION_HISTORY_PATH_INSIDE = (
    "API_SERVER_OPERATION_HISTORY_PATH_INSIDE"
)

YAML_PATH_SERVER_TIMEZONE = "server.timezone"
YAML_PATH_SERVER_HOST = "server.host"
YAML_PATH_SERVER_PORT = "server.port"
YAML_PATH_VICTORIAMETRICS_URL = "victoria_metrics.url"
YAML_PATH_LOCK_TIMEOUT_HOURS = "operations.lock_timeout_hours"


class ConfigError(Exception):
    """Custom exception for configuration errors."""

    def __init__(
        self,
        env_key: str | None = None,
        yaml_path: str | None = None,
    ) -> None:
        """Initialize the exception with an error message.

        Args:
            env_key: The missing environment variable key.
            yaml_path: The YAML path used to look up the value.

        """
        missing_env = f"Configuration parameter '{env_key}' not found in environment"
        if yaml_path:
            missing_env = f"{missing_env} or YAML path '{yaml_path}'"
        message = f"{missing_env}, and no default value provided."
        super().__init__(message)
        self.message = message


def get_config(request: Request) -> AppConfig:
    """Dependency injection for application config.

    Retrieves the config from the app state, which is populated
    during the FastAPI lifespan context.

    Args:
        request: FastAPI request object containing app state

    Returns:
        Application configuration instance

    """
    return request.app.state.config


def get_param(
    env_key: str,
    omegaconf: DictConfig | ListConfig,
    yaml_path: str | None,
    default: str | None,
) -> str:
    """Get parameter from environment variable or use default.

    Args:
        env_key: Environment variable key
        omegaconf: OmegaConf configuration object
        yaml_path: Parameter path from YAML config
        default: Default value

    Returns:
        Parameter value from environment

    Raises:
        ConfigError: If parameter is not found in environment or YAML and no
            default is provided

    """
    ret_param = None
    try:
        ret_param = os.environ[env_key]
    except KeyError as err:
        yaml_param = None
        if yaml_path:
            yaml_param = omegaconf
            for part in yaml_path.split("."):
                if isinstance(yaml_param, (DictConfig)):
                    yaml_param = yaml_param.get(part, None)
                else:
                    yaml_param = None

                if yaml_param is None:
                    break
        if isinstance(yaml_param, (str, int)):
            ret_param = str(yaml_param)
        elif default is not None:
            ret_param = default
        else:  # ret_param is None
            raise ConfigError(env_key=env_key, yaml_path=yaml_path) from err

    return ret_param


def init_config() -> AppConfig:
    """Initialize application configuration.

    Returns:
        Application configuration instance

    """
    # Get primary environment variable
    api_server_config_path = os.getenv(
        ENV_API_SERVER_CONFIG_PATH_INSIDE, DEFAULT_API_SERVER_CONFIG_PATH_INSIDE
    )
    api_server_logging_config_path = os.getenv(
        ENV_API_SERVER_LOGGING_CONFIG_PATH_INSIDE,
        DEFAULT_API_SERVER_LOGGING_CONFIG_PATH_INSIDE,
    )
    api_server_logging_dir_path = os.getenv(
        ENV_API_SERVER_LOGGING_DIR_PATH_INSIDE,
        DEFAULT_API_SERVER_LOGGING_DIR_PATH_INSIDE,
    )
    api_server_operation_history_path = os.getenv(
        ENV_API_SERVER_OPERATION_HISTORY_PATH_INSIDE,
        DEFAULT_API_SERVER_OPERATION_HISTORY_PATH_INSIDE,
    )

    # Get YAML configuration
    try:
        conf = OmegaConf.load(api_server_config_path)
    except FileNotFoundError:
        conf = OmegaConf.create()

    # Get configuration parameters from config file
    api_server_timezone = get_param(
        ENV_API_SERVER_TIMEZONE,
        conf,
        YAML_PATH_SERVER_TIMEZONE,
        DEFAULT_SERVER_TIMEZONE,
    )
    api_server_host = get_param(
        ENV_API_SERVER_HOST, conf, YAML_PATH_SERVER_HOST, DEFAULT_SERVER_HOST
    )
    api_server_port = int(
        get_param(
            ENV_API_SERVER_PORT, conf, YAML_PATH_SERVER_PORT, str(DEFAULT_SERVER_PORT)
        )
    )
    victoria_metrics_url = get_param(
        ENV_VICTORIAMETRICS_URL, conf, YAML_PATH_VICTORIAMETRICS_URL, None
    )
    lock_timeout_hours = int(
        get_param(
            ENV_API_SERVER_LOCK_TIMEOUT_HOURS,
            conf,
            YAML_PATH_LOCK_TIMEOUT_HOURS,
            str(DEFAULT_LOCK_TIMEOUT_HOURS),
        )
    )

    return AppConfig(
        server=ServerConfig(
            timezone=api_server_timezone, host=api_server_host, port=api_server_port
        ),
        log=LoggingConfig(
            logging_config_path=api_server_logging_config_path,
            logging_dir_path=api_server_logging_dir_path,
        ),
        victoria_metrics_url=victoria_metrics_url,
        lock_timeout_hours=lock_timeout_hours,
        operation_history_path=api_server_operation_history_path,
    )
