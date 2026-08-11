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

from typing import Any

from dapr.clients import DaprClient
from fastapi import FastAPI
from pydantic import TypeAdapter
from pydantic_handlebars import render

from drasi.reaction.models.ChangeEvent import ChangeEvent
from drasi.reaction.models.ChangeNotification import ChangeNotification
from drasi.reaction.models.ChangeNotification import Op
from drasi.reaction.models.ChangePayload import ChangePayload
from drasi.reaction.models.ChangeSource import ChangeSource
from drasi.reaction.models.ControlEvent import ControlEvent
from drasi.reaction.sdk import AsyncChangeEventFunc, AsyncControlEventFunc, DrasiReaction
from drasi.reaction.utils import yaml_query_configs

from subscription import SubscriptionRegistry
from utils.types import PubSubPayload, PubSubConsumerConfig, ReactionConfig

_OP_TO_PAYLOAD_SHAPE_FIELD = {
    Op.i: "added",
    Op.u: "updated",
    Op.d: "deleted",
}


class PubSubRouter():
    """
    Dispatches Drasi events to Dapr pub/sub topics.
    """

    def __init__(
        self,
        dapr_client: DaprClient,
        app: FastAPI,
        pubsub_name: str,
        subscription_registry: SubscriptionRegistry,
        name: str = "drasi-pubsub-router",
    ) -> None:
        """
        Initialize a PubSubRouter instance.
        
        Args:
            dapr_client (DaprClient): The Dapr client for interacting with Dapr.
            app (FastAPI): The FastAPI application instance.
            pubsub_name (str): The name of the Dapr pub/sub component.
            subscription_registry (SubscriptionRegistry): The subscription registry.
            name (str): The name of the router. Defaults to "drasi-pubsub-router".
        """
        self._name = name
        self._dapr_client = dapr_client
        self._pubsub_name = pubsub_name
        self._subscription_registry = subscription_registry
        self._reaction = DrasiReaction(
            on_change_event=self._make_on_change_event(),
            on_control_event=self._make_on_control_event(),
            parse_query_configs=yaml_query_configs,
            app=app,
            host="0.0.0.0",
        )
        self._query_config_adapter = TypeAdapter(ReactionConfig)


    @property
    def reaction_config(self) -> ReactionConfig:
        """
        Get the current supported queries lazily.

        Returns:
            dict[str, QueryConfig]: A dictionary mapping query IDs to their corresponding information.
        """
        # TODO: needs to fallback to unpacked format
        # TODO: may want to provide a better shape but would not be the same obj
        return self._reaction.query_configs


    def start(self) -> None:
        """Start the router (blocking)."""
        self._reaction.start()
    
        
    def shutdown(self) -> None:
        """Shutdown the router (idempotent)."""
        pass


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

 
    def _publish_to_pubsub(self, pubsub_name: str, topic: str, message: str) -> None:
        """
        Publish a message to a Dapr pub/sub topic.

        Args:
            pubsub_name (str): The name of the Dapr pubsub component.
            topic (str): The name of the topic on which to publish the message.
            message (str): The message to publish.
        """

        self._dapr_client.publish_event(
            pubsub_name,
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
        # TODO: make this generalizable
        # Assume template always has a "task" field for now
        task = render(template.task, context)

        return PubSubPayload(task=task)


    def _make_on_change_event(self) -> AsyncChangeEventFunc:
        async def on_change_event(event: ChangeEvent, query_config: dict[str, Any] | None) -> None:
            if query_config is None:
                return

            # TODO: check if payload shape is given and fill with defaults if needed

            # Convert to unpacked form
            unpacked_events = self._to_unpacked_events(event)
            if not unpacked_events:
                return

            # Get all consumers for the query
            all_consumers = await self._subscription_registry.get_subscriptions(event.queryId)

            # Filter for consumers that are interested in this event type (if they have a payload shape field for it)
            # TODO: don't hardcode event op index
            op = unpacked_events[0].op
            # TODO: make this responsibility of subscription registry?
            # payload shape field may be `None`
            consumers = [
                consumer for consumer in all_consumers if getattr(consumer, _OP_TO_PAYLOAD_SHAPE_FIELD.get(op), None) is not None
            ]
    
            # Apply template to events based on event type (added, updated, deleted)
            bindings: list[tuple[PubSubConsumerConfig, PubSubPayload | ChangeNotification]] = []

            for evt in unpacked_events:
                for consumer in consumers:
                    payload = self._make_payload(consumer, evt)
                    bindings.append((consumer, payload))

            # Publish to Dapr pubsub
            # TODO: parallelize this
            for consumer, payload in bindings:
                self._publish_to_pubsub(self._pubsub_name, consumer.topic, payload.model_dump_json())

        return on_change_event


    def _make_on_control_event(self) -> AsyncControlEventFunc:
        async def on_control_event(event: ControlEvent, query_config: dict[str, Any] | None) -> None:
            # TODO: implement
            pass

        return on_control_event
