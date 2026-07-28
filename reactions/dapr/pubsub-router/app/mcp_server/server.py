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
import sys

from dapr.clients import DaprClient
from fastmcp import FastMCP

from mcp_server.tools import DrasiQueryToolSet

# Use stderr instead of stdout to avoid mixing with MCP server output
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)

logger = logging.getLogger(__name__)


class MCPServer:
    """
    An MCP server that registers tools. Runs in a separate process.
    """

    def __init__(self, name: str | None = "drasi-pubsub-router-mcp") -> None:
        self._name = name
        self._dapr_client = DaprClient()
        self._mcp = FastMCP(name=self._name)
        self._tools = DrasiQueryToolSet(dapr_client=self._dapr_client, mcp=self._mcp)

        logger.info(f"MCPServer initialized with name: {self._name} and tools registered")


    def start(self) -> None:
        """
        Start the MCP server.
        """
        # TODO: make port configurable
        asyncio.run(self._mcp.run_async(transport="http", host="0.0.0.0", port=9000))


    def shutdown(self) -> None:
        """
        Shutdown the MCP server.
        """
        if self._dapr_client:
            try:
                self._dapr_client.close()
            except Exception:
                pass
            self._dapr_client = None

        if self._mcp:
            self._mcp = None


def run_mcp_server():
    """
    Entry point for the MCP server process.
    """
    try:
        server = MCPServer()
        server.start()
    finally:
        server.shutdown()
