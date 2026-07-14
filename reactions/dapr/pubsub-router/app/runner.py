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

from router import PubSubRouter


class AppRunner():
    """
    Lifecycle and dependency management for the router and MCP server.
    """


    def __init__(self) -> None:
        # TODO: shutdown handlers
        # TODO: MCP
        self._dapr_client = DaprClient()
        self._router = PubSubRouter(
            name="dapr-pubsub-router",
            dapr_client=self._dapr_client,
        )


    async def start(self) -> None:
        """
        Start the runtime.
        """
        self._router.start()


    async def shutdown(self) -> None:
        """
        Shutdown the runtime (idempotent).
        """
        
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
