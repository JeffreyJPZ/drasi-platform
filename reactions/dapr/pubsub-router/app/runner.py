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

from dapr.clients import DaprClient
from fastapi import FastAPI
from fastmcp import FastMCP
from fastmcp.utilities.lifespan import combine_lifespans

from utils.types import StateStoreConfig
from mcp_server import MCPServer
from router import PubSubRouter
from subscription import SubscriptionRegistry


class AppRunner():
    """
    Lifecycle and dependency management for the Dapr Pub/Sub Router.
    """

    def __init__(self, state_store_config: StateStoreConfig | None = None) -> None:
        """
        Initialize an AppRunner instance.

        Args:
            state_store_config (StateStoreConfig): Configuration for the state store backend.
        """
        # TODO: shutdown handlers
        self._dapr_client = DaprClient()

        self._ensure_pubsub(self._dapr_client)

        self._subscription_registry = SubscriptionRegistry(
            dapr_client=self._dapr_client,
            state_store_config=state_store_config or StateStoreConfig(),
        )

        self._app = FastAPI(title="Drasi Pub/Sub Router")
        self._router = PubSubRouter(
            dapr_client=self._dapr_client,
            app=self._app,
            pubsub_name=self._pubsub_name,
            subscription_registry=self._subscription_registry,
        )
        self._mcp = FastMCP(name="drasi-pubsub-router-mcp")
        self._mcp_server = MCPServer(
            mcp=self._mcp,
            pubsub_name=self._pubsub_name,
            subscription_registry=self._subscription_registry,
            reaction_config=self._router.reaction_config,
        )

        self._combine_routes(self._app, self._mcp)
        


    def start(self) -> None:
        """Start the runtime."""
        self._mcp_server.start()
        # Router must be started last as it blocks
        self._router.start()


    def shutdown(self) -> None:
        """Shutdown the runtime in reverse order of instantiation (idempotent)."""
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


    def _ensure_pubsub(self, dapr_client: DaprClient) -> None:
        """
        Ensure that a Dapr pubsub component is available (first-found wins).

        Args:
            dapr_client (DaprClient): The runner's Dapr client instance.

        Raises:
            RuntimeError: If no Dapr pubsub component is found.
        """
        metadata = dapr_client.get_metadata()
        registered_components = metadata.registered_components or []

        for component in registered_components:
            if "pubsub" in component.type.lower():
                self._pubsub_name = component.name
                return

        raise RuntimeError("No Dapr pubsub component found. Please ensure that a pubsub component is registered with the Dapr sidecar.")


    def _combine_routes(self, app: FastAPI, mcp: FastMCP) -> None:
        """
        Wire MCP routes onto the shared FastAPI app.

        Args:
            app (FastAPI): The runner's FastAPI app instance.
            mcp (FastMCP): The runner's FastMCP server instance containing MCP routes.
        """
        mcp_app = mcp.http_app(path="/mcp")

        app.router.routes.extend([*mcp_app.routes])
        app.router.lifespan_context = combine_lifespans(app.router.lifespan_context, mcp_app.lifespan)
