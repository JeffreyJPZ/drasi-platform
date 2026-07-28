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

import multiprocessing as mp

from dapr.clients import DaprClient

from mcp_server import run_mcp_server
from router import PubSubRouter


class AppRunner():
    """
    Lifecycle and dependency management for the router and MCP server.
    """

    def __init__(self) -> None:
        # TODO: shutdown handlers
        self._dapr_client = DaprClient()
        self._router = PubSubRouter(
            dapr_client=self._dapr_client,
        )
        self._mcp_server = mp.Process(target=run_mcp_server, args=(), daemon=True)


    def start(self) -> None:
        """
        Start the runtime.
        """
        self._mcp_server.start()
        # Pubsub router must be started after since it blocks
        self._router.start()


    def shutdown(self) -> None:
        """
        Shutdown the runtime in reverse order of instantiation (idempotent).
        """

        if self._mcp_server:
            try:
                # TODO: make shutdown more graceful
                self._mcp_server.terminate()
                self._mcp_server.join(timeout=5)
            finally:
                if self._mcp_server.is_alive():
                    self._mcp_server.kill()
                    self._mcp_server.join(timeout=5)
            self._mcp_server = None
        
        if self._router:
            try:
                self._router.shutdown()
            except Exception:
                pass
            self._router = None

        if self._dapr_client:
            try:
                self._dapr_client.close()
            except Exception:
                pass
            self._dapr_client = None
