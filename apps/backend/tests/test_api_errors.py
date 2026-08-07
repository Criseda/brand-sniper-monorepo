import json

import pytest
from api_errors import (
    _problem_response,
    database_exception_handler,
    http_exception_handler,
    validation_exception_handler,
)
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from shared_utils.db_connection import MissingDatabaseURLError
from starlette.exceptions import HTTPException


def make_request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/test",
            "headers": [],
            "query_string": b"",
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("testclient", 50000),
        }
    )


def response_body(response) -> dict:
    return json.loads(response.body)


def test_problem_response_supports_extension_statuses_and_unmatched_routes():
    response = _problem_response(make_request(), status_code=599, detail="Extension error")

    assert response.status_code == 599
    assert response_body(response)["title"] == "HTTP Error"


@pytest.mark.asyncio
async def test_http_exception_handler_stringifies_structured_detail():
    response = await http_exception_handler(
        make_request(),
        HTTPException(status_code=400, detail={"reason": "invalid"}),
    )

    assert response_body(response)["detail"] == "{'reason': 'invalid'}"


@pytest.mark.asyncio
async def test_validation_handler_omits_non_json_location_parts():
    error = {
        "type": "missing",
        "loc": ("body", object(), "name"),
        "msg": "Field required",
        "input": {},
    }

    response = await validation_exception_handler(make_request(), RequestValidationError([error]))

    assert response_body(response)["errors"][0]["location"] == ["body", "name"]


@pytest.mark.asyncio
async def test_database_handler_supports_missing_configuration():
    response = await database_exception_handler(make_request(), MissingDatabaseURLError("missing URL"))

    assert response.status_code == 503
