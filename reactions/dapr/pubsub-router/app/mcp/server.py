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

from dapr.clients import DaprClient
from fastmcp import FastMCP

from tools import DrasiQueryToolSet


class MCPServer:
    """
    An MCP server that registers tools.
    """

    def __init__(self, name: str, dapr_client: DaprClient, mcp: FastMCP):
        self._name = name
        self._tools = DrasiQueryToolSet(dapr_client, mcp)


    def start(self) -> None:
        """
        Start the MCP server.
        """
        pass


    def shutdown(self) -> None:
        """
        Shutdown the MCP server.
        """
        pass