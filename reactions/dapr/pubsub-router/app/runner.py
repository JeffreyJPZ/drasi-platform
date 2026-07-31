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
from fastapi import FastAPI
from fastmcp import FastMCP
from fastmcp.utilities.lifespan import combine_lifespans

from mcp_server import MCPServer
from router import PubSubRouter
from subscription import SubscriptionRegistry


class AppRunner():
    """
    Lifecycle and dependency management for the Dapr Pub/Sub Router.
    """

    def __init__(self) -> None:
        """Initializes an AppRunner instance."""
        # TODO: shutdown handlers
        self._dapr_client = DaprClient()
        self._subscription_registry = SubscriptionRegistry()
        self._app = FastAPI(title="Drasi Pub/Sub Router")
        self._router = PubSubRouter(
            dapr_client=self._dapr_client,
            app=self._app,
            subscription_registry=self._subscription_registry,
        )
        self._mcp = FastMCP(name="drasi-pubsub-router-mcp")
        self._mcp_server = MCPServer(
            dapr_client=self._dapr_client,
            mcp=self._mcp,
            subscription_registry=self._subscription_registry,
            reaction_config=self._router.reaction_config,
        )
        self._wire_routes(self._mcp)
        


    def start(self) -> None:
        """
        Start the runtime.
        """
        self._mcp_server.start()
        # Router must be started last as it blocks
        self._router.start()


    def shutdown(self) -> None:
        """
        Shutdown the runtime in reverse order of instantiation.
        """
        if self._router:
            try:
                self._router.shutdown()
            except Exception:
                pass
            self._router = None

        if self._mcp_server:
            try:
                self._mcp_server.shutdown()
            except Exception:
                pass
            self._mcp_server = None

        if self._dapr_client:
            try:
                self._dapr_client.close()
            except Exception:
                pass
            self._dapr_client = None


    def _wire_routes(self, mcp: FastMCP) -> None:
        """Wire MCP routes onto the shared FastAPI app."""
        mcp_app = mcp.http_app(path="/mcp")

        self._app.router.routes.extend([*mcp_app.routes])
        self._app.router.lifespan_context = combine_lifespans(self._app.router.lifespan_context, mcp_app.lifespan)
