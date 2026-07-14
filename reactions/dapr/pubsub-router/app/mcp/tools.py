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


# TODO: we may want a tool executor instead but since our toolset is limited it may not be necessary
class DrasiQueryToolSet:
    """
    A toolset for managing Drasi queries.
    """


    def __init__(self, dapr_client: DaprClient, mcp: FastMCP) -> None:
        self._dapr_client = dapr_client

        mcp.tool(self.subscribe)
        mcp.tool(self.unsubscribe)
        mcp.tool(self.list_queries)


    def subscribe(self, query_id: str, agent_id: str, pubsub_name: str, topic_name: str) -> None:
        """
        Subscribe an agent to a Drasi query on a given pub/sub component and topic.

        Args:
            query_id (str): The ID of the query to subscribe to.
            agent_id (str): The ID of the agent making the subscription.
            pubsub_name (str): The name of the Dapr pubsub component.
            topic_name (str): The name of the topic on which the agent will receive messages.
        """
        pass


    def unsubscribe(self, query_id: str, agent_id: str) -> None:
        """
        Unsubscribe an agent from a Drasi query.

        Args:
            query_id (str): The ID of the query to unsubscribe from.
            agent_id (str): The ID of the agent to unsubscribe.
        """
        pass


    def list_queries(self) -> None:
        """
        List all queries.
        """
        pass