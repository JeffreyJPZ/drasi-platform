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

from dapr.clients import DaprClient
from fastmcp import FastMCP

from mcp_server.tools import DrasiQueryToolSet
from subscription import SubscriptionRegistry
from utils.types import ReactionConfig

logger = logging.getLogger(__name__)


class MCPServer:
    """
    An MCP server that registers tools.
    """

    def __init__(self,
        mcp: FastMCP,
        pubsub_name: str,
        subscription_registry: SubscriptionRegistry,
        reaction_config: ReactionConfig,
        name: str = "drasi-pubsub-router-mcp"
    ) -> None:
        """
        Initialize an MCPServer instance.

        Args:
            mcp (FastMCP): The FastMCP server instance.
            subscription_registry (SubscriptionRegistry): The subscription registry.
            reaction_config (ReactionConfig): The static Drasi reaction configuration.
            name (str): The name of the server. Defaults to "drasi-pubsub-router-mcp".
        """
        self._name = name
        self._tools = DrasiQueryToolSet(
            mcp=mcp,
            pubsub_name=pubsub_name,
            subscription_registry=subscription_registry,
            reaction_config=reaction_config,
        )

        logger.info(f"MCPServer initialized with name: {self._name} and tools registered")


    def start(self) -> None:
        """Start the MCP server."""
        pass


    def shutdown(self) -> None:
        """Shutdown the MCP server."""
        pass
