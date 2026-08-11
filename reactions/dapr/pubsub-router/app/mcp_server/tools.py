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

import asyncio
import logging
from typing import Literal

from fastmcp import FastMCP

from subscription import SubscriptionRegistry
from utils.types import PubSubConsumerConfig, PubSubPayload, QueryConfig, ReactionConfig

logger = logging.getLogger(__name__)

# TODO: make this an enum
type Operation = Literal["added", "updated", "deleted"]

_OPERATION_FIELDS: tuple[Operation, ...] = ("added", "updated", "deleted")


# TODO: we may want a tool executor instead but since our toolset is limited it may not be necessary
class DrasiQueryToolSet:
    """
    A toolset for managing Drasi queries.
    """

    def __init__(
        self,
        mcp: FastMCP,
        pubsub_name: str,
        subscription_registry: SubscriptionRegistry,
        reaction_config: ReactionConfig,
    ) -> None:
        """
        Initialize a DrasiQueryToolSet instance.

        Args:
            mcp (FastMCP): The FastMCP server instance.
            pubsub_name (str): The name of the Dapr pub/sub component.
            subscription_registry (SubscriptionRegistry): The subscription registry.
            reaction_config (ReactionConfig): The static Drasi reaction configuration.
        """
        self._pubsub_name = pubsub_name
        self._subscription_registry = subscription_registry
        self._reaction_config = reaction_config

        mcp.tool(self.subscribe)
        mcp.tool(self.unsubscribe)
        mcp.tool(self.list_queries)
        mcp.tool(self.list_subscriptions)

        logger.info("DrasiQueryToolSet initialized and tools registered with MCP")


    def _normalize_operations(self, operations: str | list[str] | None) -> list[Operation]:
        """
        Normalize operation input to a deduplicated ordered list of subscription fields.

        Args:
            operations (str | list[str] | None): The operations to normalize. None means all operations.
        
        Returns:
            list[Operation]: A deduplicated ordered list of normalized operations.
        """
        if operations is None:
            return list(_OPERATION_FIELDS)

        raw_operations = [operations] if isinstance(operations, str) else list(operations)
        normalized_operations: list[Operation] = []

        for operation in raw_operations:
            normalized_operation = operation.strip().lower()
            if normalized_operation not in _OPERATION_FIELDS:
                raise ValueError(
                    f"Unsupported operation '{operation}'. Expected one of: {', '.join(_OPERATION_FIELDS)}"
                )
            if normalized_operation not in normalized_operations:
                normalized_operations.append(normalized_operation)

        if not normalized_operations:
            raise ValueError("operations must not be empty")

        return normalized_operations


    def _get_query_payload_template(
        self,
        query_config: QueryConfig,
        operation: Operation
    ) -> PubSubPayload | None:
        """
        Read a payload template for an operation from the query config.

        Args:
            query_config (QueryConfig): The query configuration.
            operation (Operation): The operation for which to get the payload template.
        
        Returns:
            PubSubPayload | None: The payload template for the operation, or None if not provided.
        """
        payload = getattr(query_config, operation, None)  
        return PubSubPayload.model_validate(payload) if payload is not None else None


    async def subscribe(
        self,
        query_id: str,
        agent_id: str,
        topic: str,
        operations: str | list[str] | None = None,
    ) -> str:
        """
        Subscribe an agent to a Drasi query on a given pub/sub topic.

        Args:
            query_id (str): The ID of the query to subscribe to.
            agent_id (str): The ID of the agent making the subscription.
            topic (str): The name of the topic on which the agent will receive messages.
            operations (str | list[str] | None): The operations to subscribe to. None means all operations.
        """
        requested_operations = self._normalize_operations(operations)
        logger.info(
            f"Subscribing agent '{agent_id}' "
            f"to query '{query_id}' "
            f"on (pubsub '{self._pubsub_name}', topic '{topic}', operations={requested_operations})",
        )

        query_config = self._reaction_config.get(query_id)
        if query_config is None:
            raise ValueError(f"Unknown query_id '{query_id}'")

        consumer_config_kwargs: dict[str, PubSubPayload | str | None] = {
            "id": agent_id,
            "topic": topic,
        }
        for operation in requested_operations:
            consumer_config_kwargs[operation] = self._get_query_payload_template(query_config, operation)

        await self._subscription_registry.upsert_subscription(
            query_id=query_id,
            consumer_id=agent_id,
            config=PubSubConsumerConfig(**consumer_config_kwargs),
        )
    
        return (
            f"Agent '{agent_id}' successfully subscribed to query '{query_id}' "
            f"on (pubsub '{self._pubsub_name}', topic '{topic}', operations={requested_operations})"
        )


    async def unsubscribe(
        self,
        query_id: str,
        agent_id: str,
        operations: str | list[str] | None = None,
    ) -> str:
        """
        Unsubscribe an agent from a Drasi query.

        Args:
            query_id (str): The ID of the query to unsubscribe from.
            agent_id (str): The ID of the agent to unsubscribe.
            operations (str | list[str] | None): The operations to unsubscribe from. None means all operations.
        """
        requested_operations = self._normalize_operations(operations)
        logger.info(
            f"Unsubscribing agent '{agent_id}' from query '{query_id}' for operations={requested_operations}",
        )

        if self._reaction_config.get(query_id) is None:
            raise ValueError(f"Unknown query_id '{query_id}'")

        existing_subscription = await self._subscription_registry.get_subscription(
            query_id=query_id,
            consumer_id=agent_id,
        )

        if existing_subscription is None:
            return (
                f"Agent '{agent_id}' successfully unsubscribed from query '{query_id}' "
                f"for operations={requested_operations}"
            )

        updates = {operation: None for operation in requested_operations}
        await self._subscription_registry.update_subscription(
            query_id=query_id,
            consumer_id=agent_id,
            fields=updates,
        )

        return (
            f"Agent '{agent_id}' successfully unsubscribed from query '{query_id}' "
            f"for operations={requested_operations}"
        )


    async def list_queries(self) -> list[str]:
        """
        List all queries.
        """
        logger.info("Listing all queries")
        # TODO: does this need to be from query API?
        return [query_id for query_id in self._reaction_config.keys()]


    async def list_subscriptions(self, agent_id: str) -> list[PubSubConsumerConfig]:
        """
        List all subscriptions for an agent.

        Args:
            agent_id (str): The ID of the agent for which to list subscriptions.
        """
        logger.info("Listing all subscriptions")

        # TODO: optimize
        # Check all query IDs
        subscription_tasks = [self._subscription_registry.get_subscription(query_id, agent_id) for query_id in self._reaction_config.keys()]
        results = await asyncio.gather(*subscription_tasks)

        return [result for result in results if result is not None]
