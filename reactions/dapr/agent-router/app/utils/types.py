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
from typing import Annotated, Literal

from pydantic import BaseModel, Field, RootModel


class Operation(StrEnum):
    """Enumeration of supported Drasi operations."""

    ADDED = "added"
    UPDATED = "updated"
    DELETED = "deleted"


class StateStoreConfig(BaseModel):
    """
    Configuration for a state store.

    Attributes:
        type (Literal["in-memory", "dapr"]): The state store backend to use. If omitted, defaults to "dapr".
        store_name (str): The name of the Dapr state store component to use. Ignored if `type` is not "dapr".
    """

    type: Literal["in-memory", "dapr"] = Field(default="dapr")
    store_name: str | None = None


# TODO: should "packed" events be supported?
class PubSubPayload(BaseModel):
    """
    Payload for a message to be published to a Dapr pub/sub topic.
    Note: the shape currently matches `TriggerAction` from Dapr Agents.

    Attributes:
        task (str): The task message as a template string.
    """

    task: str


class QueryConfig(BaseModel):
    """
    Static configuration for a Drasi query.

    Attributes:
        name (str): A human-readable name for the query.
        description (str): A human-readable description of the query.
        added (PubSubPayload | None): Optional payload shape for added events.
        updated (PubSubPayload | None): Optional payload shape for updated events.
        deleted (PubSubPayload | None): Optional payload shape for deleted events.
    """

    name: str
    description: str
    added: PubSubPayload | None = None
    updated: PubSubPayload | None = None
    deleted: PubSubPayload | None = None


# Static configuration — matches the shape of the reaction YAML
ReactionConfig = dict[str, QueryConfig]


class PubSubConsumerConfig(BaseModel):
    """
    Subscription configuration for a registered agent.

    Attributes:
        id (str): A unique identifier for the agent.
        topic (str): The name of the topic on which to publish events.
        added (PubSubPayload | None): Optional payload shape for added events. Resolved with the static config if omitted.
        updated (PubSubPayload | None): Optional payload shape for updated events. Resolved with the static config if omitted.
        deleted (PubSubPayload | None): Optional payload shape for deleted events. Resolved with the static config if omitted.
    """

    id: str
    topic: str
    added: PubSubPayload | None = None
    updated: PubSubPayload | None = None
    deleted: PubSubPayload | None = None


# TODO: can this be optimized?
QuerySubscriptionState = RootModel[
    Annotated[dict[str, PubSubConsumerConfig], Field(default_factory=dict)]
]
