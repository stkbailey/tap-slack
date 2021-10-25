"""Stream type classes for tap-slack."""

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Iterable
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_slack.client import SlackStream
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
        selected_channel_types = ["public_channel"] # "mpim", "private_channel", "im"
        params["exclude_archived"] = False
        params["types"] = ",".join(selected_channel_types)
        return params


class ChannelMembersStream(SlackStream):
    name = "channel_members"
    parent_stream_type = ChannelsStream
    path = "/conversations.members"
    primary_keys = ["channel_id", "id"]
    records_jsonpath = "members.[*]"
    schema = schemas.channel_members

    ignore_parent_replication_keys = True

    def parse_response(self, response):
        user_list = extract_jsonpath(self.records_jsonpath, input=response.json())
        yield from ({"member_id": ii} for ii in user_list)

    @property
    def state_partitioning_keys(self):
        "Remove partitioning keys to prevent state logging for individual threads."
        return []

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
    def threads_stream_starting_timestamp(self, context):
        lookback_days = timedelta(self.config["thread_lookback_days"])
        return datetime.now(tz=timezone.utc) - lookback_days

    @property
    def messages_stream_starting_timestamp(self, context):
        return super().get_starting_timestamp(context)

    def get_url_params(self, context, next_page_token):
        """Augment default to implement incremental syncing."""
        params = super().get_url_params(context, next_page_token)
        start_time = self.get_starting_timestamp(context)
        if start_time:
            params["oldest"] = start_time.strftime("%s")
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """
        Directly invoke the threads stream sync on relevant messages,
        and filter out messages that have already been synced before.
        """
        if row.get("thread_ts") and self._tap.streams["threads"].selected:
            threads_context = {**context, **{"thread_ts": row["ts"]}}
            self._tap.streams["threads"].sync(context=threads_context)
        if row["ts"] < self.messages_stream_starting_timestamp:
            return None
        return row

    def get_starting_timestamp(self, context: Optional[dict]) -> Optional[datetime]:
        """
        Threads can continue to have messages for weeks after the original message
        was posted, so we cannot assume that we have scraped all message replies
        at the same time we scrape the original message. This function will return
        the starting timestamp for the EARLIEST of either the regular starting timestamp
        (e.g. for full syncs) or the THREAD_LOOKBACK_DAYS days before the current run.
        A longer THREAD_LOOKBACK_DAYS will result in longer incremental sync runs.
        """
        if not self.messages_stream_starting_timestamp:
            return None
        elif self.threads_stream_starting_timestamp < self.messages_stream_starting_timestamp:
            return self.threads_stream_starting_timestamp
        else:
            return self.messages_stream_starting_timestamp


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

    @property
    def state_partitioning_keys(self):
        "Remove partitioning keys to prevent state logging for individual threads."
        return []

class UsersStream(SlackStream):
    name = "users"
    path = "/users.list"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "members.[*]"
    schema = schemas.users
