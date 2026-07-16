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

from dataclasses import field
from typing import Any

from dapr.clients import DaprClient
from pydantic import BaseModel, TypeAdapter
from pydantic_handlebars import render

from drasi.reaction.models.ChangeEvent import ChangeEvent
from drasi.reaction.models.ChangeNotification import ChangeNotification
from drasi.reaction.models.ChangeNotification import Op
from drasi.reaction.models.ChangePayload import ChangePayload
from drasi.reaction.models.ChangeSource import ChangeSource
from drasi.reaction.models.ControlEvent import ControlEvent
from drasi.reaction.sdk import AsyncChangeEventFunc, AsyncControlEventFunc, DrasiReaction
from drasi.reaction.utils import yaml_query_configs

_OP_TO_PAYLOAD_SHAPE_FIELD = {
    Op.i: "added",
    Op.u: "updated",
    Op.d: "deleted",
}


# TODO: should template and payload be separate?
class PubSubPayload(BaseModel):
    """
    Payload for a message to be published to a Dapr pub/sub topic.
    Note: the shape currently matches `TriggerAction` from Dapr Agents.

    Attributes:
        task (str): The task message as a template string.
    """

    task: str


class PubSubConsumerConfig(BaseModel):
    """
    Configuration for a Drasi consumer.
    If no payload shape is provided, an "unpacked" Drasi event will be published to the topic.

    Attributes:
        pubsub (str): The name of the Dapr pubsub component.
        topic (str): The name of the topic on which to publish events.
        added (PubSubPayload | None): Optional payload shape for added events.
        updated (PubSubPayload | None): Optional payload shape for updated events.
        deleted (PubSubPayload | None): Optional payload shape for deleted events.
    """

    pubsub: str
    topic: str
    added: PubSubPayload | None = None
    updated: PubSubPayload | None = None
    deleted: PubSubPayload | None = None


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
        self._reaction = DrasiReaction(
            on_change_event=self._make_on_change_event(),
            on_control_event=self._make_on_control_event(),
            parse_query_configs=yaml_query_configs,
        )
        self._query_config_adapter = TypeAdapter(dict[str, PubSubConsumerConfig])


    def _to_unpacked_events(self, event: ChangeEvent) -> list[ChangeNotification]:
        """
        Converts a single "packed" Drasi event (containing batched changes) into a flat list
        of "unpacked" events, one per changed item.

        Assumptions:
        - addedResults items are plain "after" records
        - deletedResults items are plain "before" records
        - updatedResults items are dicts/objects with "before" and "after" keys
        """
        source = ChangeSource(
            queryId=event.queryId,
            ts_ms=event.sourceTimeMs,
        )
        # TODO: events are not guaranteed to arrive in order of sequence number?
        # Kept constant for now so validation works
        seq = 0

        unpacked_events: list[ChangeNotification] = []

        for item in event.addedResults:
            after = item.model_dump()
            unpacked_events.append(
                ChangeNotification(
                    op=Op.i,
                    ts_ms=event.sourceTimeMs,
                    seq=seq,
                    payload=ChangePayload(source=source, before=None, after=after),
                )
            )

        for item in event.updatedResults:
            # Assume item has "before" and "after" attributes
            before = item.before.model_dump() if item.before is not None else None
            after = item.after.model_dump() if item.after is not None else None
            unpacked_events.append(
                ChangeNotification(
                    op=Op.u,
                    ts_ms=event.sourceTimeMs,
                    seq=seq,
                    payload=ChangePayload(source=source, before=before, after=after),
                )
            )

        for item in event.deletedResults:
            before = item.model_dump()
            unpacked_events.append(
                ChangeNotification(
                    op=Op.d,
                    ts_ms=event.sourceTimeMs,
                    seq=seq,
                    payload=ChangePayload(source=source, before=before, after=None),
                )
            )

        return unpacked_events

 
    def _publish_to_pubsub(self, pubsub: str, topic: str, message: str) -> None:
        """
        Publish a message to a Dapr pub/sub topic.

        Args:
            pubsub (str): The name of the Dapr pubsub component.
            topic (str): The name of the topic on which to publish the message.
            message (str): The message to publish.
        """

        self._dapr_client.publish_event(
            pubsub,
            topic,
            message,
            data_content_type="application/json",
        )


    def _make_payload(self, consumer: PubSubConsumerConfig, event: ChangeNotification) -> PubSubPayload | ChangeNotification:
        """
        Apply a template to a Drasi event to generate a payload for Dapr pub/sub.

        Args:
            consumer (PubSubConsumerConfig): The consumer configuration containing the templates.
            event (ChangeNotification): The Drasi event to which the template is applied.

        Returns:
            PubSubPayload | ChangeNotification: The generated payload, or the original event if no template is applicable.
        """
        
        field = _OP_TO_PAYLOAD_SHAPE_FIELD.get(event.op)
        template = getattr(consumer, field) if field else None

        if template is None:
            return event  # No applicable template, return the original event

        context = event.model_dump()
        # Assume template always has a "task" field for now
        task = render(template.task, context)

        return PubSubPayload(task=task)


    def _make_on_change_event(self) -> AsyncChangeEventFunc:
        async def on_change_event(event: ChangeEvent, query_config: dict[str, Any] | None) -> None:
            if query_config is None:
                return

            # Convert to unpacked form
            unpacked_events = self._to_unpacked_events(event)
            
            # Coerce query config to more usable form
            config = self._query_config_adapter.validate_python(query_config)

            # Get the list of consumers that are subscribed to this query ID
            consumers = list(config.values())

            # Apply template to events based on event type (added, updated, deleted)
            bindings: list[tuple[PubSubConsumerConfig, PubSubPayload | ChangeNotification]] = []

            for payload in unpacked_events:
                for consumer in consumers:
                    payload = self._make_payload(consumer, payload)
                    bindings.append((consumer, payload))

            # Publish to Dapr pubsub
            # TODO: parallelize this
            for consumer, payload in bindings:
                self._publish_to_pubsub(consumer.pubsub, consumer.topic, payload.model_dump_json())

        return on_change_event


    def _make_on_control_event(self) -> AsyncControlEventFunc:
        async def on_control_event(event: ControlEvent, query_config: dict[str, Any] | None) -> None:
            # TODO: implement
            pass

        return on_control_event


    def start(self) -> None:
        """Start the router (blocking)."""
        self._reaction.start()

    
    def shutdown(self) -> None:
        """Shutdown the router (idempotent)."""
        pass
