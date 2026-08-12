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
from utils.types import Operation, PubSubPayload, PubSubConsumerConfig, ReactionConfig


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
            dapr_client (DaprClient): Injected Dapr client.
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
        return self._reaction.query_configs


    def start(self) -> None:
        """Start the router (blocking)."""
        self._reaction.start()
    
        
    def shutdown(self) -> None:
        """Shutdown the router (idempotent)."""
        pass


    def _get_change_event_operation(self, event: ChangeEvent) -> Operation | None:
        """
        Determine the operation type of a Drasi change event.

        Args:
            event (ChangeEvent): The Drasi change event.
        
        Returns:
            Operation | None: The operation type (Operation.ADDED, Operation.UPDATED, Operation.DELETED) or None if the event does not match any known operation.
        """
        if event.addedResults:
            return Operation.ADDED
        elif event.updatedResults:
            return Operation.UPDATED
        elif event.deletedResults:
            return Operation.DELETED
        else:
            return None


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
        # TODO: sequence numbers for "unpacked" events are currently not reliable since they are generated reaction-side,
        # clients should perform their own deduplication based on payload content or similar.
        # This sequence number is a dummy to enable validation.
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

        # TODO: may want opt-in signing
        self._dapr_client.publish_event(
            pubsub_name=pubsub_name,
            topic=topic,
            data=message,
            data_content_type="application/json",
        )


    def _make_payload(
        self,
        consumer: PubSubConsumerConfig,
        event: ChangeNotification,
        operation: Operation,
    ) -> PubSubPayload | ChangeNotification:
        """
        Apply a template to an unpacked Drasi event to generate a payload for Dapr pub/sub.

        Args:
            consumer (PubSubConsumerConfig): The consumer configuration containing the templates.
            event (ChangeNotification): The unpacked Drasi event to which the template is applied.
            operation (Operation): The operation being performed.

        Returns:
            PubSubPayload | ChangeNotification: The generated payload, or the umodified unpacked event if no template is applicable.
        """
        
        # Assumes operation matches the template field name in the consumer config
        template = getattr(consumer, operation.value) if operation else None

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

            # Convert to unpacked form
            unpacked_events = self._to_unpacked_events(event)
            if not unpacked_events:
                return

            operation = self._get_change_event_operation(event)
            if not operation:
                # Defensive guard - shouldn't happen
                return

            # Get all consumers that are interested in this event type (if they have a payload template field)
            consumers = await self._subscription_registry.get_subscriptions(
                query_id=event.queryId,
                operations=operation,
            )
    
            # Apply template to events based on event type (added, updated, deleted)
            bindings: list[tuple[PubSubConsumerConfig, PubSubPayload | ChangeNotification]] = []

            for evt in unpacked_events:
                for consumer in consumers:
                    payload = self._make_payload(
                        consumer=consumer,
                        event=evt,
                        operation=operation,
                    )
                    bindings.append((consumer, payload))

            # Publish to Dapr pubsub
            # TODO: may want to parallelize or bulk publish if ordering is not important
            for consumer, payload in bindings:
                self._publish_to_pubsub(
                    pubsub_name=self._pubsub_name,
                    topic=consumer.topic,
                    payload=payload.model_dump_json()
                )

        return on_change_event


    def _make_on_control_event(self) -> AsyncControlEventFunc:
        async def on_control_event(event: ControlEvent, query_config: dict[str, Any] | None) -> None:
            # TODO: implement
            pass

        return on_control_event
