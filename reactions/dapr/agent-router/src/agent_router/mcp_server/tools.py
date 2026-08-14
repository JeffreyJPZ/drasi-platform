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
from fastmcp.exceptions import ToolError
from pydantic import Field

from agent_router.subscription import SubscriptionRegistry
from agent_router.utils.types import (
    EventType,
    ListQueriesResult,
    QueryConfig,
    QueryResult,
    SubscribeResult,
    UnsubscribeResult,
)

logger = logging.getLogger(__name__)


# TODO: fix docstrings
# TODO: better error handling
class AgentRouterToolset:
    """
    Toolset for the Drasi Agent Router MCP server.
    """

    def __init__(
        self,
        mcp: FastMCP,
        subscription_registry: SubscriptionRegistry,
        query_configs: dict[str, QueryConfig],
    ) -> None:
        """
        Initialize an AgentRouterToolset instance.

        Args:
            mcp (FastMCP): Injected FastMCP server instance.
            subscription_registry (SubscriptionRegistry): Injected subscription registry.
            query_configs (dict[str, QueryConfig]): Static configuration for all queries.
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
            query_id (str): Unique identifier of the query to subscribe to.
            agent_id (str): Unique identifier of the agent making the subscription.
            topic (str): Name of the topic on which the agent will receive messages.
            event_types (list[EventType]): List of event types to which the agent is subscribed.
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
            raise ToolError(f"Unknown query_id '{query_id}'")

        # TODO: verify that the agent is allowed to subscribe to the query
        subscription_id = self._subscription_registry.new_subscription_id(agent_id)

        await self._subscription_registry.upsert_subscription(
            query_id=query_id,
            subscription_id=subscription_id,
            topic=topic,
            event_types=event_types,
        )
    
        logger.info(
            f"Agent '{agent_id}' successfully subscribed to query '{query_id}' "
            f"on topic '{topic}', subscription_id='{subscription_id}', event_types={event_types!r}"
        )

        return SubscribeResult(
            agent_id=agent_id,
            query_id=query_id,
            topic=topic,
            subscription_id=subscription_id,
            event_types=event_types,
        )


    async def unsubscribe(
        self,
        query_id: str,
        agent_id: str,
        subscription_id: str,
    ) -> UnsubscribeResult:
        """
        Unsubscribe an agent from a Drasi query.

        Args:
            query_id (str): Unique identifier of the query to unsubscribe from.
            agent_id (str): Unique identifier of the agent to unsubscribe.
            subscription_id (str): Unique identifier of the subscription to remove.
        """
        logger.info(
            f"Unsubscribing agent '{agent_id}' from query '{query_id}', subscription_id='{subscription_id}'",
        )

        if self._query_configs.get(query_id) is None:
            raise ToolError(f"Unknown query_id '{query_id}'")

        # TODO: verify that the agent actually owns the subscription
        subscription = await self._subscription_registry.get_subscription(
            query_id=query_id,
            subscription_id=subscription_id,
        )

        if subscription is None:
            return (
                f"Agent '{agent_id}' successfully unsubscribed from query '{query_id}', "
                f"subscription_id='{subscription_id}'(no existing subscription found)"
            )

        await self._subscription_registry.delete_subscription(
            query_id=query_id,
            subscription_id=subscription_id,
        )

        logger.info(
            f"Agent '{agent_id}' successfully unsubscribed from query '{query_id}', "
            f"subscription_id='{subscription_id}'"
        )

        return UnsubscribeResult(
            agent_id=agent_id,
            query_id=query_id,
            subscription_id=subscription_id,
        )


    async def list_queries(self) -> ListQueriesResult:
        """
        List all Drasi queries.

        Returns:
            list[QueryConfig]: A list of all query configurations.
        """
        logger.info("Listing all queries")

        return ListQueriesResult(
            queries=[
                QueryResult(
                    query_id=query_id,
                    title=self._query_configs.get(query_id, {}).title,
                    description=self._query_configs.get(query_id, {}).description,
                )
                for query_id in self._query_configs.keys()
            ]
        )
