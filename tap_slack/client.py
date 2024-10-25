"""REST client handling, including SlackStream base class."""

from __future__ import annotations

import time
from abc import ABC
from typing import Any, Dict, List, Optional, Text

from requests import Response
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.pagination import JSONPathPaginator, BasePageNumberPaginator
from singer_sdk.streams import RESTStream


class ThrottledJSONPathPaginator(JSONPathPaginator):
    """A throttled paginator."""

    max_requests_per_minute = 200

    def get_next(self, response: Response) -> str | None:
        """Get the next page token.

        Args:
            response: API response object.

        Returns:
            The next page token.
        """
        if self.max_requests_per_minute:
            time.sleep(60.0 / self.max_requests_per_minute)
        return super().get_next(response)


class ThrottledPageNumberPaginator(BasePageNumberPaginator):
    """Paginator for Slack Logs."""

    max_requests_per_minute = 200

    def has_more(self, response: Response) -> bool:
        """Has more pages.

        Args:
            response: API response object.

        Returns:
            Boolean flag used to indicate if the endpoint has more pages.
        """
        data = response.json()
        max_page = data["paging"]["pages"]
        current_page = data["paging"]["page"]
        return current_page != max_page

    def get_next(self, response: Response) -> str | None:
        """Get the next page token.

        Args:
            response: API response object.

        Returns:
            The next page number.
        """
        if self.max_requests_per_minute:
            time.sleep(60.0 / self.max_requests_per_minute)

        data = response.json()
        max_page = data["paging"]["pages"]
        current_page = data["paging"]["page"]
        next_page = None
        if current_page < max_page:
            next_page = current_page + 1

        return next_page


class SlackStream(RESTStream):
    """Slack stream class."""

    _page_size = 500
    url_base = "https://slack.com/api"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$.response_metadata.next_cursor"

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object."""
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.config.get("api_key"),
        )

    @property
    def expectations(self) -> List[str]:
        return [
            "stream__returns_record",
            "stream__record_schema_matches_catalog",
            "stream__primary_key",
        ]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {}
        if next_page_token:
            params["cursor"] = next_page_token
        if self._page_size:
            params["limit"] = self._page_size
        if context and "channel_id" in context:
            params["channel"] = context["channel_id"]
        if context and "thread_ts" in context:
            params["ts"] = context["thread_ts"]
        return params

    def get_new_paginator(self):
        """Override default to apply rate throttling for streams."""
        return ThrottledJSONPathPaginator(self.next_page_token_jsonpath)
