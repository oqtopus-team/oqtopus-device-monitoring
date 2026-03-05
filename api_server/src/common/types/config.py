from pydantic import BaseModel


class ServerConfig(BaseModel):
    """Configuration for the API server."""

    timezone: str
    host: str
    port: int


class LoggingConfig(BaseModel):
    """Configuration for logging."""

    logging_config_path: str
    logging_dir_path: str


class AppConfig(BaseModel):
    """Application configuration settings."""

    server: ServerConfig
    log: LoggingConfig
    victoria_metrics_url: str
    lock_timeout_hours: int
    operation_history_path: str
