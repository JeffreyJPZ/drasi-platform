#
# Copyright 2025 The Drasi Authors.
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

from pydantic import BaseModel

# TODO: should template and payload be separate?
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
        added (PubSubPayload | None): Optional default payload shape for added events.
            Defaults to Drasi's "unpacked" event format.
        updated (PubSubPayload | None): Optional default payload shape for updated events.
            Defaults to Drasi's "unpacked" event format.
        deleted (PubSubPayload | None): Optional default payload shape for deleted events.
            Defaults to Drasi's "unpacked" event format.
    """

    name: str
    description: str
    added: PubSubPayload | None = None
    updated: PubSubPayload | None = None
    deleted: PubSubPayload | None = None


ReactionConfig = dict[str, QueryConfig]


class PubSubConsumerConfig(BaseModel):
    """
    Subscription configuration for a Drasi consumer.

    Attributes:
        id (str): A unique identifier for the consumer.
        pubsub (str): The name of the Dapr pubsub component.
        topic (str): The name of the topic on which to publish events.
        added (PubSubPayload | None): Optional payload shape for added events. Resolved with the static config if omitted.
        updated (PubSubPayload | None): Optional payload shape for updated events. Resolved with the static config if omitted.
        deleted (PubSubPayload | None): Optional payload shape for deleted events. Resolved with the static config if omitted.
    """

    id: str
    pubsub: str
    topic: str
    added: PubSubPayload | None = None
    updated: PubSubPayload | None = None
    deleted: PubSubPayload | None = None
