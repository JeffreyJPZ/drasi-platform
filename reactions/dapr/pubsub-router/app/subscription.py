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

import logging

from dapr.clients import DaprClient

from stores.dapr import DaprStateStore
from stores.memory import InMemoryStateStore
from utils.types import PubSubConsumerConfig, QuerySubscriptionState

logger = logging.getLogger(__name__)


class SubscriptionRegistry():
    """
    Subscription management for the Dapr Pub/Sub Router.
    """

    def __init__(
        self,
        dapr_client: DaprClient,
        use_state_store: bool = True,  # TODO: inject state store instead
    ) -> None:
        """
        Initialize a SubscriptionRegistry instance.

        Args:
            dapr_client (DaprClient): Injected Dapr client.
            use_state_store (bool): Whether to use a state store for persistent subscriptions. Defaults to True.
        """
        self._dapr_client = dapr_client
        self._use_state_store = use_state_store
        # TODO: make this a default factory for testing
        self._state_model_cls = QuerySubscriptionState

        # TODO: does this branching belong here?
        if self._use_state_store:
            self._state_store = DaprStateStore[self._state_model_cls](
                dapr_client=dapr_client,
                state_model_cls=self._state_model_cls,
            )
        else:
            self._state_store = InMemoryStateStore[self._state_model_cls](
                state_model_cls=self._state_model_cls,
            )


    async def get_subscription(self, query_id: str, consumer_id: str) -> PubSubConsumerConfig | None:
        """
        Get a subscription for a given query ID and consumer ID.

        Args:
            query_id (str): The query ID for which the subscription is being retrieved.
            consumer_id (str): The ID of the consumer whose subscription is being retrieved.
        """
        state = await self._state_store.get_state(query_id)
        subscriptions = state.root if state is not None else {}

        return subscriptions.get(consumer_id, None)


    async def get_subscriptions(self, query_id: str) -> list[PubSubConsumerConfig]:
        """
        Get all subscriptions for a given query ID.

        Args:
            query_id (str): The query ID for which the subscription is being retrieved.
        """
        state = await self._state_store.get_state(query_id)
        subscriptions = state.root if state is not None else {}
        
        return list(subscriptions.values())


    async def add_subscription(self, query_id: str, config: PubSubConsumerConfig) -> None:
        """
        Add a subscription for a given query ID.

        Args:
            query_id (str): The query ID for which the subscription is being added.
            config (PubSubConsumerConfig): The configuration for the subscription containing a consumer ID.
        """
        state = await self._state_store.get_state(query_id)
        subscriptions = state.root if state is not None else {}
        
        if config.id in subscriptions:
            return  # Subscription already exists

        subscriptions[config.id] = config

        await self._state_store.save_state(query_id, state)     


    async def delete_subscription(self, query_id: str, consumer_id: str) -> None:
        """
        Delete a subscription for a given consumer.

        Args:
            query_id (str): The query ID for which the subscription is being deleted.
            consumer_id (str): The ID of the consumer whose subscription is being deleted.
        """
        state = await self._state_store.get_state(query_id)
        subscriptions = state.root if state is not None else {}

        subscriptions.pop(consumer_id, None)

        await self._state_store.save_state(query_id, state)
