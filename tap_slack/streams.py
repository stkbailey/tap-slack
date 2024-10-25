"""Stream type classes for tap-slack."""
import requests
import pendulum
import time

from datetime import datetime, timezone, timedelta
from typing import Optional, cast, Any, Dict
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_slack.client import SlackStream, ThrottledPageNumberPaginator
from tap_slack import schemas


class ChannelsStream(SlackStream):
    name = "channels"
    path = "/conversations.list"
    primary_keys = ["id"]
    records_jsonpath = "channels.[*]"
    schema = schemas.channels

    def get_child_context(self, record, context):
        """Return context dictionary for child stream."""
        return {"channel_id": record["id"]}

    def get_url_params(self, context, next_page_token):
        """Augment default to filter channel types to return and extract messages from."""
        params = super().get_url_params(context, next_page_token)
        params["exclude_archived"] = False
        params["types"] = ",".join(self.config["channel_types"])
        return params

    def post_process(self, row, context):
        "Join the channel if not a member, but emit no data."
        row = super().post_process(row, context)
        # return all in selected_channels or default to all, exclude any in excluded_channels list
        channel_id = row["id"]
        if self._is_channel_included(channel_id):
            if not row["is_member"] and self.config.get("auto_join_channels", False):
                self._join_channel(channel_id)
            return row

    def _is_channel_included(self, channel_id: str) -> bool:
        selected_channels = self.config.get("selected_channels")
        excluded_channels = self.config.get("excluded_channels", [])
        if channel_id in excluded_channels:
                return False
        if selected_channels and channel_id not in selected_channels:
                return False
        return True

    def _join_channel(self, channel_id: str) -> requests.Response:
        url = f"{self.url_base}/conversations.join"
        params = {"channel": channel_id}
        response = self.requests_session.post(
            url=url, params=params, headers=self.authenticator.auth_headers
        )
        if not response.json().get("ok"):
            self.logger.warning(
                "Error joining channel %s: %s", response.json().get("error")
            )
        self.logger.info("Successfully joined channel: %s", channel_id)


class ChannelMembersStream(SlackStream):
    name = "channel_members"
    parent_stream_type = ChannelsStream
    path = "/conversations.members"
    primary_keys = ["channel_id", "member_id"]
    records_jsonpath = "members.[*]"
    schema = schemas.channel_members

    ignore_parent_replication_keys = True
    state_partitioning_keys = []

    def parse_response(self, response):
        user_list = extract_jsonpath(self.records_jsonpath, input=response.json())
        yield from ({"member_id": ii} for ii in user_list)

    def post_process(self, row, context=None):
        row = super().post_process(row, context=context)
        row["channel_id"] = context.get("channel_id")
        return row


class MessagesStream(SlackStream):
    name = "messages"
    parent_stream_type = ChannelsStream
    path = "/conversations.history"
    primary_keys = ["channel_id", "ts"]
    replication_key = "ts"
    records_jsonpath = "messages.[*]"
    schema = schemas.messages

    ignore_parent_replication_key = True
    max_requests_per_minute = 50

    @property
    def threads_stream_start(self):
        lookback_days = timedelta(days=self.config["thread_lookback_days"])
        start_date = datetime.now(tz=timezone.utc) - lookback_days
        return start_date.timestamp()

    def get_url_params(self, context, next_page_token):
        """Augment default to implement incremental syncing."""
        params = super().get_url_params(context, next_page_token)
        start_timestamp = self.get_starting_replication_key_value(context)
        if start_timestamp:
            params["oldest"] = start_timestamp
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """
        Directly invoke the threads stream sync on relevant messages,
        and filter out messages that have already been synced before.
        """
        threads_stream = self._tap.streams["threads"]
        replication_key_ts = self.get_starting_replication_key_value(context)
        if row.get("thread_ts") and threads_stream.selected:
            threads_context = {**context, **{"thread_ts": row["ts"]}}
            threads_stream.sync(context=threads_context)
            time.sleep(60.0 / threads_stream.max_requests_per_minute)
        if row["ts"] and float(row["ts"]) < replication_key_ts:
            return None
        return row

    def get_starting_replication_key_value(
            self, context: Optional[dict]
    ) -> Optional[int]:
        """
        Threads can continue to have messages for weeks after the original message
        was posted, so we cannot assume that we have scraped all message replies
        at the same time we scrape the original message. This function will return
        the starting timestamp for the EARLIEST of either the regular starting timestamp
        (e.g. for full syncs) or the THREAD_LOOKBACK_DAYS days before the current run.
        A longer THREAD_LOOKBACK_DAYS will result in longer incremental sync runs.
        """
        state = self.get_context_state(context)
        replication_key_value = state.get("replication_key_value")
        if replication_key_value:
            if self.threads_stream_start < float(replication_key_value):
                return self.threads_stream_start
            return float(replication_key_value)
        elif "start_date" in self.config:
            start_date = cast(datetime, pendulum.parse(self.config["start_date"]))
            return start_date.replace(tzinfo=timezone.utc).timestamp()
        else:
            self.logger.info(
                "Setting replication value to 0 to perform full historical sync."
            )
            return 0.0


class ThreadsStream(SlackStream):
    """
    The threads stream is directly invoked by the Messages stream, but not via
    standard parent-child relationship. Instead, parsed messages that have a
    more recent "last_reply_at" timestamp will have a FULL_TABLE sync performed.
    """

    name = "threads"
    path = "/conversations.replies"
    primary_keys = ["channel_id", "thread_ts", "ts"]
    records_jsonpath = "messages.[*]"
    max_requests_per_minute = 50
    schema = schemas.threads

    state_partitioning_keys = []

    def post_process(self, row, context=None):
        row = super().post_process(row, context=context)
        row["channel_id"] = context.get("channel_id")
        return row


class UsersStream(SlackStream):
    name = "users"
    path = "/users.list"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "members.[*]"
    schema = schemas.users


class IntegrationLogsStream(SlackStream):
    name = "integration_logs"
    path = "/team.integrationLogs"
    primary_keys = ["user_id"]
    replication_key = None
    records_jsonpath = "logs.[*]"
    schema = schemas.integration_logs
    _page_size = 500

    def get_new_paginator(self):
        return ThrottledPageNumberPaginator(1)

