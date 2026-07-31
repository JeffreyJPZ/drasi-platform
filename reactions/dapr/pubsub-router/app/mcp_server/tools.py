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

import asyncio
import logging

from dapr.clients import DaprClient
from fastmcp import FastMCP

from subscription import SubscriptionRegistry
from utils.types import PubSubConsumerConfig, PubSubPayload, QueryConfig, ReactionConfig

logger = logging.getLogger(__name__)


# TODO: we may want a tool executor instead but since our toolset is limited it may not be necessary
class DrasiQueryToolSet:
    """
    A toolset for managing Drasi queries.
    """

    def __init__(
        self,
        dapr_client: DaprClient,
        mcp: FastMCP,
        subscription_registry: SubscriptionRegistry,
        reaction_config: ReactionConfig,
    ) -> None:
        """
        Initializes a DrasiQueryToolSet instance.

        Args:
            dapr_client (DaprClient): The Dapr client for interacting with Dapr.
            mcp (FastMCP): The FastMCP instance.
            subscription_registry (SubscriptionRegistry): The subscription registry.
            reaction_config (ReactionConfig): The static configuration for the Drasi queries.
        """
        self._dapr_client = dapr_client
        self._subscription_registry = subscription_registry
        self._reaction_config = reaction_config

        mcp.tool(self.subscribe)
        mcp.tool(self.unsubscribe)
        mcp.tool(self.list_queries)
        mcp.tool(self.list_subscriptions)

        logger.info("DrasiQueryToolSet initialized and tools registered with MCP")


    async def subscribe(self, query_id: str, agent_id: str, pubsub_name: str, topic_name: str) -> str:
        """
        Subscribe an agent to a Drasi query on a given pub/sub component and topic.

        Args:
            query_id (str): The ID of the query to subscribe to.
            agent_id (str): The ID of the agent making the subscription.
            pubsub_name (str): The name of the Dapr pubsub component.
            topic_name (str): The name of the topic on which the agent will receive messages.
        """
        logger.info(f"Subscribing agent '{agent_id}' to query '{query_id}' on (pubsub '{pubsub_name}', topic '{topic_name}')")

        # TODO: validate query_id exists in reaction_config

        # TODO: clean this up; agents may only be interested in certain types of events, maybe use op type?
        # Only payload shape fields for event types that are defined in the reaction config for this query
        added = self._reaction_config.get(query_id, {}).get("added", None)
        updated = self._reaction_config.get(query_id, {}).get("updated", None)
        deleted = self._reaction_config.get(query_id, {}).get("deleted", None)

        await self._subscription_registry.add_subscription(
            query_id=query_id,
            config=PubSubConsumerConfig(
                id=agent_id,
                pubsub=pubsub_name,
                topic=topic_name,
                added=PubSubPayload.model_validate(added) if added is not None else None,
                updated=PubSubPayload.model_validate(updated) if updated is not None else None,
                deleted=PubSubPayload.model_validate(deleted) if deleted is not None else None,
            )
        )
    
        return f"Agent '{agent_id}' successfully subscribed to query '{query_id}' on (pubsub '{pubsub_name}', topic '{topic_name}')"


    async def unsubscribe(self, query_id: str, agent_id: str) -> str:
        """
        Unsubscribe an agent from a Drasi query.

        Args:
            query_id (str): The ID of the query to unsubscribe from.
            agent_id (str): The ID of the agent to unsubscribe.
        """
        logger.info(f"Unsubscribing agent '{agent_id}' from query '{query_id}'")

        # TODO: validate query_id exists in reaction_config

        await self._subscription_registry.remove_subscription(
            query_id=query_id,
            consumer_id=agent_id
        )

        return f"Agent '{agent_id}' successfully unsubscribed from query '{query_id}'"


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
