"""Identity-safe, read-only web fallback extractors."""

from .browser import BrowserProviderConfigurationError, BrowserRunResult, PlaywrightWebFallbackProvider
from .dom import DOMExtractionResult, extract_dom_candidates
from .identity import (
    DuplicateJSONKeyError,
    IdentityCheck,
    IdentityError,
    ImmutableRequestContext,
    ObservationCandidate,
    Target,
    make_request_context,
    parse_json_unique,
    parse_positive_id,
    portrait_token_from_profile_href,
    sanitize_source_url,
    validate_request_context,
)
from .network import NetworkExtractionResult, extract_nested_floor

__all__ = [
    "BrowserProviderConfigurationError",
    "BrowserRunResult",
    "DOMExtractionResult",
    "DuplicateJSONKeyError",
    "IdentityCheck",
    "IdentityError",
    "ImmutableRequestContext",
    "NetworkExtractionResult",
    "ObservationCandidate",
    "PlaywrightWebFallbackProvider",
    "Target",
    "extract_dom_candidates",
    "extract_nested_floor",
    "make_request_context",
    "parse_json_unique",
    "parse_positive_id",
    "portrait_token_from_profile_href",
    "sanitize_source_url",
    "validate_request_context",
]
