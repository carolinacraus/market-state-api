# market_pipeline/errors.py
from __future__ import annotations

class PipelineError(Exception):
    http_status = 500
    code = "PIPELINE_ERROR"

    def __init__(self, message: str, *, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

class BadRequestError(PipelineError):
    http_status = 400
    code = "BAD_REQUEST"

class UnauthorizedError(PipelineError):
    http_status = 401
    code = "UNAUTHORIZED"

class UpstreamAPIError(PipelineError):
    http_status = 502
    code = "UPSTREAM_API_ERROR"

class NotFoundError(PipelineError):
    http_status = 404
    code = "NOT_FOUND"
