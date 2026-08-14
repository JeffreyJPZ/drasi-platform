#
# Copyright 2026 The Drasi Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, Field, RootModel


# ------------------------------------------
# Query models
# ------------------------------------------


# TODO: consider moving these into a `types` package
class EventType(StrEnum):
    """
    Enumeration of supported event types.
    
    ADDED: A record was added to the query result set.
    UPDATED: A record in the query result set was updated.
    DELETED: A record was removed from the query result set.
    """

    ADDED = "added"
    UPDATED = "updated"
    DELETED = "deleted"


# ------------------------------------------
# Static query configuration models
# ------------------------------------------


class QueryConfig(BaseModel):
    """
    Static query configuration for a Drasi query.

    Attributes:
        title (str): A human-readable name for the query.
        description (str): A human-readable description of the query.
    """

    title: str
    description: str


# ------------------------------------------
# Runner configuration models
# ------------------------------------------


class PubSubConfig(BaseModel):
    """
    Configuration for pub/sub.

    Attributes:
        pubsub_name (str): The name of the Dapr pub/sub component to use.
    """

    pubsub_name: str | None = None


class StateConfig(BaseModel):
    """
    Configuration for subscription state.

    Attributes:
        store_name (str): The name of the Dapr state store component to use for persistence.
        state_key_prefix (str): Optional prefix to use for state keys to avoid collisions.
    """

    store_name: str | None = None
    state_key_prefix: str | None = None


# ------------------------------------------
# Subscription state models
# ------------------------------------------


class QuerySubscription(BaseModel):
    """
    Subscription configuration.

    Attributes:
        id (str): A unique identifier for the subscription (this may not be the same as the agent's).
        query_id (str): The unique identifier for the query targeted by the subscription.
        topic (str): The name of the topic on which to publish events.
        event_types (list[EventType]): The list of event types that the subscription is interested in.
            Must contain at least one event type.
    """

    id: str
    query_id: str
    topic: str
    event_types: Annotated[list[EventType], Field(min_length=1)]


# TODO: can this be optimized?
class QuerySubscriptionState(RootModel[dict[str, QuerySubscription]]):
    root: dict[str, QuerySubscription] = Field(default_factory=dict)


# ------------------------------------------
# Tool result models
# ------------------------------------------


# TODO: what should the result look like
class SubscribeResult(BaseModel):
    """
    Result of a `subscribe` tool call.

    Attributes:
        query_id (str): The unique identifier for the query.
        subscription_id (str): The unique identifier for the subscription.
    """
    query_id: str
    subscription_id: str


# TODO: what should the result look like
class UnsubscribeResult(BaseModel):
    """
    Result of an `unsubscribe` tool call.

    Attributes:
        query_id (str): The unique identifier for the query.
        subscription_id (str): The unique identifier for the subscription.
    """
    query_id: str
    subscription_id: str


class ListQueriesResult(BaseModel):
    """
    Result of a `list_queries` tool call.

    Attributes:
        query_id (str): The unique identifier for the query.
        title (str): A human-readable name for the query.
        description (str): A human-readable description of the query.
    """
    query_id: str
    title: str
    description: str
