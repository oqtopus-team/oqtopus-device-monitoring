import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from common.config import init_config
from common.logger import setup_logging
from common.victoria_metrics import VictoriaMetricsClient
from routers import meta, metrics
from schemas.errors import ErrorResponse

# Initialize logging before any logger usage
app_logger = logging.getLogger("api-server")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """FastAPI lifespan context manager for startup/shutdown events.

    Loads configuration on startup and makes it available via app.state.

    Args:
        app: FastAPI application instance

    """
    # Startup: Load application configuration
    try:
        config = init_config()
    except Exception as e:
        app_logger.critical("Failed to load application configuration: %s", e)
        raise

    app.state.config = config

    client: VictoriaMetricsClient | None = None
    try:
        # Initialize VictoriaMetrics client
        client = VictoriaMetricsClient(config.victoria_metrics_url)
        app.state.client = client
        yield
    finally:
        if client is not None:
            await client.close()


app = FastAPI(lifespan=lifespan)

# register routers
app.include_router(metrics.router)
app.include_router(meta.router)


# add handlers
@app.exception_handler(RequestValidationError)
def validation_exception_handler(
    _: Request, exc: RequestValidationError
) -> JSONResponse:
    """Handle request validation errors and return a structured JSON response.

    Args:
        _: The incoming HTTP request.
        exc: The validation error exception.

    Returns:
        JSONResponse: A JSON response with status code 400 and error message.

    """
    return ErrorResponse(
        status_code=400,
        content={"message": str(exc)},
    )


if __name__ == "__main__":
    # load server configuration
    try:
        config = init_config()
    except Exception as e:
        app_logger.critical("Failed to load application configuration: %s", e)
        raise

    # Setup logging
    setup_logging(config.log, tz_str=config.server.timezone)

    # Start server
    uvicorn.run(app, host=config.server.host, port=config.server.port)
