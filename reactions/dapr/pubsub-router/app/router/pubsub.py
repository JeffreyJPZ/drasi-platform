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

from typing import Any, Callable

from dapr.clients import DaprClient
from pydantic import BaseModel, RootModel
from pydantic.types import Json
from pydantic_handlebars import render

from reactions.sdk.python.drasi.reaction.models.ChangeEvent import ChangeEvent
from reactions.sdk.python.drasi.reaction.models.ChangeNotification import ChangeNotification
from reactions.sdk.python.drasi.reaction.models.ChangeNotification import Op
from reactions.sdk.python.drasi.reaction.models.ChangePayload import ChangePayload
from reactions.sdk.python.drasi.reaction.models.ChangeSource import ChangeSource
from reactions.sdk.python.drasi.reaction.models.ControlEvent import ControlEvent
from reactions.sdk.python.drasi.reaction.sdk import DrasiReaction


class PubSubPromptTemplate(BaseModel):
    """
    Template for generating messages to be published to Dapr pub/sub topics.
    """

    template: str  # The template string


class PubSubConsumerConfig(BaseModel):
    """
    Configuration for a consumer of Drasi events.

    Attributes:
        pubsub (str): The name of the Dapr pubsub component.
        topic (str): The name of the topic on which to publish events.
        queryId (str): The Drasi query ID that this consumer is interested in.
    """

    pubsub: str
    topic: str
    queryId: str


class PubSubQueryConfigEntry(BaseModel):
    """
    Configuration for a Drasi query that specifies how to route events to Dapr pub/sub topics.

    Attributes:
        consumers (Json[list[PubSubConsumerConfig]]): A stringified list of consumer configurations
        added (PubSubPromptTemplate | None): Optional template for added events
        updated (PubSubPromptTemplate | None): Optional template for updated events
        deleted (PubSubPromptTemplate | None): Optional template for deleted events
    """

    consumers: Json[list[PubSubConsumerConfig]]
    added: PubSubPromptTemplate | None = None
    updated: PubSubPromptTemplate | None = None
    deleted: PubSubPromptTemplate | None = None


class PubSubQueryConfig(RootModel[dict[str, PubSubQueryConfigEntry]]):
    """
    Mapping of Drasi query IDs to their corresponding pub/sub routing configurations.
    """

    def __iter__(self):
        return iter(self.root.keys())

    def __getitem__(self, item):
        if item in self.root:
            return self.root[item]
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{item}'")
    
    def __getattr__(self, item):
        if item in self.root:
            return self.root[item]
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{item}'")
    

class PubSubPayload(BaseModel):
    """
    Payload for a message to be published to a Dapr pub/sub topic.
    Note: the shape currently matches `TriggerAction` from Dapr Agents.

    Attributes:
        task (str): The task message
    """

    task: str


class PubSubRouter():
    """
    Dispatches Drasi events to Dapr pub/sub topics.
    """


    def __init__(
        self,
        name: str,
        dapr_client: DaprClient,
    ) -> None:
        self._name = name
        self._dapr_client = dapr_client
        self._reaction = DrasiReaction()

        self._reaction.on_change_event = self._make_on_change_event()
        self._reaction.on_control_event = self._make_on_control_event()


    def to_unpacked_events(event: ChangeEvent) -> list[ChangeNotification]:
        """
        Converts a single packed Drasi event (containing batched changes) into a flat list
        of unpacked events, one per changed item.

        Assumptions:
        - addedResults items are plain "after" records
        - deletedResults items are plain "before" records
        - updatedResults items are dicts/objects with "before" and "after" keys
        """
        source = ChangeSource(
            queryId=event.queryId,
            ts_ms=event.sourceTimeMs,
        )

        unpacked_events: list[ChangeNotification] = []

        for item in event.addedResults:
            unpacked_events.append(
                ChangeNotification(
                    op=Op.i,
                    ts_ms=event.sourceTimeMs,
                    payload=ChangePayload(source=source, before=None, after=item),
                )
            )

        for item in event.updatedResults:
            # item is expected to have .before / .after (or ["before"] / ["after"])
            before = getattr(item, "before", None) or item.get("before")
            after = getattr(item, "after", None) or item.get("after")
            unpacked_events.append(
                ChangeNotification(
                    op=Op.u,
                    ts_ms=event.sourceTimeMs,
                    payload=ChangePayload(source=source, before=before, after=after),
                )
            )

        for item in event.deletedResults:
            unpacked_events.append(
                ChangeNotification(
                    op=Op.d,
                    ts_ms=event.sourceTimeMs,
                    payload=ChangePayload(source=source, before=item, after=None),
                )
            )

        return unpacked_events

    
    def _publish_to_pubsub(self, pubsub_name: str, topic_name: str, message: str) -> None:
        """
        Publish a message to a Dapr pub/sub topic synchronously.

        Args:
            pubsub_name (str): The name of the Dapr pubsub component.
            topic_name (str): The name of the topic on which to publish the message.
            message (str): The message to publish.
        """

        self._dapr_client.publish_event(
            pubsub_name,
            topic_name,
            message,
            data_content_type="application/json",
        )

    
    def _make_payload(self, config: PubSubQueryConfig, event: ChangeNotification) -> PubSubPayload | None:
        """
        Apply a template to a Drasi event to generate a payload for Dapr pub/sub.

        Args:
            config (PubSubQueryConfig): The query configuration containing the templates.
            event (ChangeNotification): The Drasi event to which the template is applied.

        Returns:
            PubSubPayload | None: The generated payload, or None if no template is applicable.
        """

        match event.op:
            case Op.i if config.added:
                template = config.added.template if config.added else None
                context = {"after": event.payload.after.root, "queryId": event.payload.source.queryId}
            case Op.u if config.updated:
                template = config.updated.template if config.updated else None
                context = {"before": event.payload.before.root, "after": event.payload.after.root, "queryId": event.payload.source.queryId}
            case Op.d if config.deleted:
                template = config.deleted.template if config.deleted else None
                context = {"before": event.payload.before.root, "queryId": event.payload.source.queryId}
            case _:
                return

        task = render(template, context)

        return PubSubPayload(task=task)


    def _make_on_change_event(self) -> Callable[[ChangeEvent, dict[str, Any] | None], None]:
        def on_change_event(event: ChangeEvent, query_config: dict[str, Any] | None) -> None:
            # Assume static query config is
            #    query id:
            #       consumers: list of pubsub, topic
            #       added (optional): template
            #       updated (optional): template
            #       deleted (optional): template
            
            if query_config is None:
                return
            
            # Coerce query config to more usable form
            config = PubSubQueryConfig.model_validate(query_config)

            # Get the list of consumers that are subscribed to this query ID
            subscribed_consumers = [
                consumer for consumer in config.consumers if event.queryId == consumer.queryId
            ]

            # Convert to unpacked form
            unpacked_events = self.to_unpacked_events(event)

            # Apply template to events based on event type (added, updated, deleted)
            bindings: list[tuple[PubSubConsumerConfig, PubSubPayload]] = []
            
            for payload in unpacked_events:
                for consumer in subscribed_consumers:
                    payload = self._make_payload(config, payload)
                    if payload:
                        bindings.append((consumer, payload))

            # Publish to Dapr pubsub
            for consumer, payload in bindings:
                self._publish_to_pubsub(consumer.pubsub, consumer.topic, payload.model_dump_json())

        return on_change_event


    def _make_on_control_event(self) -> Callable[[ControlEvent, dict[str, Any] | None], None]:
        def on_control_event(event: ControlEvent, query_config: dict[str, Any] | None) -> None:
            # TODO: implement
            pass

        return on_control_event


    def start(self) -> None:
        """Start the router (blocking)."""
        self._reaction.start()

    
    def shutdown(self) -> None:
        """Shutdown the router (idempotent)."""
        pass
