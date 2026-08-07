from collections.abc import Mapping
from http import HTTPStatus
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from shared_utils import get_logger
from shared_utils.db_connection import DatabaseConnectionError, MissingDatabaseURLError
from starlette.exceptions import HTTPException as StarletteHTTPException
from telemetry import backend_http_errors_total

logger = get_logger("backend.api_errors")


class ValidationProblem(BaseModel):
    """A sanitized request-validation failure safe to return to API clients."""

    location: list[str | int]
    message: str
    error_type: str


class ProblemDetail(BaseModel):
    """RFC 9457 problem detail returned by the backend API."""

    type: str = "about:blank"
    title: str
    status: int
    detail: str
    instance: str
    errors: list[ValidationProblem] | None = None


def _problem_response(
    request: Request,
    *,
    status_code: int,
    detail: str,
    headers: Mapping[str, str] | None = None,
    errors: list[ValidationProblem] | None = None,
) -> JSONResponse:
    try:
        title = HTTPStatus(status_code).phrase
    except ValueError:
        title = "HTTP Error"

    route = getattr(request.scope.get("route"), "path", "unmatched")
    backend_http_errors_total.labels(route=route, status_code=str(status_code)).inc()

    problem = ProblemDetail(
        title=title,
        status=status_code,
        detail=detail,
        instance=f"urn:uuid:{uuid4()}",
        errors=errors,
    )
    return JSONResponse(
        status_code=status_code,
        content=problem.model_dump(mode="json", exclude_none=True),
        headers=headers,
        media_type="application/problem+json",
    )


async def http_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, StarletteHTTPException):
        raise TypeError("http_exception_handler requires an HTTPException")
    detail = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
    return _problem_response(
        request,
        status_code=exc.status_code,
        detail=detail,
        headers=exc.headers,
    )


async def validation_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, RequestValidationError):
        raise TypeError("validation_exception_handler requires a RequestValidationError")
    errors = [
        ValidationProblem(
            location=[part for part in error["loc"] if isinstance(part, (str, int))],
            message=error["msg"],
            error_type=error["type"],
        )
        for error in exc.errors()
    ]
    return _problem_response(
        request,
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        detail="The request payload is invalid.",
        errors=errors,
    )


async def database_exception_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    if not isinstance(exc, (DatabaseConnectionError, MissingDatabaseURLError)):
        raise TypeError("database_exception_handler requires a database configuration or connection error")
    logger.error(
        "[API] Database dependency unavailable for %s %s",
        request.method,
        request.url.path,
        exc_info=(type(exc), exc, exc.__traceback__),
    )
    return _problem_response(
        request,
        status_code=HTTPStatus.SERVICE_UNAVAILABLE,
        detail="The database service is temporarily unavailable.",
    )


async def unexpected_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error(
        "[API] Unexpected failure for %s %s",
        request.method,
        request.url.path,
        exc_info=(type(exc), exc, exc.__traceback__),
    )
    return _problem_response(
        request,
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        detail="An unexpected internal error occurred.",
    )


def register_error_handlers(app: FastAPI) -> None:
    """Registers the backend's stable API error contract in one place."""
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(DatabaseConnectionError, database_exception_handler)
    app.add_exception_handler(MissingDatabaseURLError, database_exception_handler)
    app.add_exception_handler(Exception, unexpected_exception_handler)


def problem_response(description: str) -> dict[str, Any]:
    """Builds an OpenAPI response entry using the problem-details media type."""
    return {
        "description": description,
        "content": {"application/problem+json": {"schema": ProblemDetail.model_json_schema()}},
    }


COMMON_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    HTTPStatus.UNPROCESSABLE_ENTITY: problem_response("Invalid request payload"),
    HTTPStatus.SERVICE_UNAVAILABLE: problem_response("Database service unavailable"),
    HTTPStatus.INTERNAL_SERVER_ERROR: problem_response("Unexpected internal error"),
}
