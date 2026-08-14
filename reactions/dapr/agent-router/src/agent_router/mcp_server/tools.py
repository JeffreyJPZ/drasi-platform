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

import logging
from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field

from agent_router.subscription import SubscriptionRegistry
from agent_router.utils.types import EventType, QueryConfig

logger = logging.getLogger(__name__)


# TODO: we may want a tool executor instead but since our toolset is limited it may not be necessary
# TODO: fix docstrings and return structured responses
class DrasiQueryToolSet:
    """
    A toolset for managing Drasi queries.
    """

    def __init__(
        self,
        mcp: FastMCP,
        subscription_registry: SubscriptionRegistry,
        query_configs: dict[str, QueryConfig],
    ) -> None:
        """
        Initialize a DrasiQueryToolSet instance.

        Args:
            mcp (FastMCP): The FastMCP server instance.
            subscription_registry (SubscriptionRegistry): The subscription registry.
            query_configs (dict[str, QueryConfig]): The static configuration for all queries.
        """
        self._subscription_registry = subscription_registry
        self._query_configs = query_configs

        mcp.tool(self.subscribe)
        mcp.tool(self.unsubscribe)
        mcp.tool(self.list_queries)

        logger.info("DrasiQueryToolSet initialized and tools registered with MCP")


    async def subscribe(
        self,
        query_id: str,
        agent_id: str,
        topic: str,
        event_types: Annotated[list[EventType], Field(min_length=1)],
    ) -> str:
        """
        Subscribe an agent to a Drasi query on a given pub/sub topic.

        Args:
            query_id (str): The ID of the query to subscribe to.
            agent_id (str): The ID of the agent making the subscription.
            topic (str): The name of the topic on which the agent will receive messages.
            event_types (list[EventType]): The list of event types to which the agent is subscribed.
                Must contain at least one event type.
        """
        # Deduplicate event types
        event_types = list(dict.fromkeys(event_types))

        logger.info(
            f"Subscribing agent '{agent_id}' "
            f"to query '{query_id}' "
            f"on topic '{topic}', "
            f"event_types={event_types!r}",
        )

        if self._query_configs.get(query_id) is None:
            raise ValueError(f"Unknown query_id '{query_id}'")

        await self._subscription_registry.upsert_subscription(
            query_id=query_id,
            subscription_id=agent_id,
            topic=topic,
            event_types=event_types,
        )
    
        return (
            f"Agent '{agent_id}' successfully subscribed to query '{query_id}' "
            f"on topic '{topic}', event_types={event_types!r})"
        )


    async def unsubscribe(
        self,
        query_id: str,
        agent_id: str,
        event_types: Annotated[list[EventType], Field(min_length=1)],
    ) -> str:
        """
        Unsubscribe an agent from a Drasi query.

        Args:
            query_id (str): The ID of the query to unsubscribe from.
            agent_id (str): The ID of the agent to unsubscribe.
            event_types (list[EventType]): The list of event types to unsubscribe from.
                Must contain at least one event type.
        """
        # Deduplicate event types
        event_types = list(dict.fromkeys(event_types))

        logger.info(
            f"Unsubscribing agent '{agent_id}' from query '{query_id}' for event_types={event_types!r}",
        )

        if self._query_configs.get(query_id) is None:
            raise ValueError(f"Unknown query_id '{query_id}'")

        current_sub = await self._subscription_registry.get_subscription(
            query_id=query_id,
            subscription_id=agent_id,
        )

        if current_sub is None:
            return (
                f"Agent '{agent_id}' successfully unsubscribed from query '{query_id}' "
                f"for event_types={event_types!r} (no existing subscription found)"
            )

        # Diff event types to determine the new set of event types for the subscription
        deleted_types = set(event_types)
        remaining_types = [event_type for event_type in current_sub.event_types if event_type not in deleted_types]

        # If no event types remain, remove the subscription to preserve the event type invariant
        if not remaining_types:
            await self._subscription_registry.delete_subscription(
                query_id=query_id,
                subscription_id=agent_id,
            )
        else:
            await self._subscription_registry.update_subscription(
                query_id=query_id,
                subscription_id=agent_id,
                topic=current_sub.topic,
                event_types=remaining_types,
            )

        return (
            f"Agent '{agent_id}' successfully unsubscribed from query '{query_id}' "
            f"for event_types={event_types!r}"
        )


    async def list_queries(self) -> list[QueryConfig]:
        """
        List all Drasi queries.
        """
        logger.info("Listing all queries")

        return [query_id for query_id in self._query_configs.keys()]
