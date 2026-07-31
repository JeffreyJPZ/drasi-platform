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

from utils.types import PubSubConsumerConfig


class SubscriptionRegistry():
    """
    Manages subscriptions for the Dapr Pub/Sub Router.
    """

    def __init__(self) -> None:
        """Initializes a SubscriptionRegistry instance."""
        # TODO: use sorted lists?
        self._lock = asyncio.Lock()
        self._subscriptions: dict[str, list[PubSubConsumerConfig]] = {}


    async def get_subscription(self, query_id: str, consumer_id: str) -> PubSubConsumerConfig | None:
        """
        Retrieves a subscription for a given query ID and consumer ID.

        Args:
            query_id (str): The query ID for which the subscription is being retrieved.
            consumer_id (str): The ID of the consumer whose subscription is being retrieved.
        """
        async with self._lock:
            consumers = self._subscriptions.get(query_id, [])
            if not consumers:
                return None

            for consumer in consumers:
                if consumer.id == consumer_id:
                    # TODO: make this a read-only copy
                    return consumer

        return None


    async def get_subscriptions(self, query_id: str) -> list[PubSubConsumerConfig]:
        """
        Retrieves a subscription for a given query ID.

        Args:
            query_id (str): The query ID for which the subscription is being retrieved.
        """
        async with self._lock:
            return self._subscriptions.get(query_id, [])


    async def add_subscription(self, query_id: str, config: PubSubConsumerConfig) -> None:
        """
        Adds a subscription for a given query ID.

        Args:
            query_id (str): The query ID for which the subscription is being added.
            config (PubSubConsumerConfig): The configuration for the subscription.
        """
        async with self._lock:
            if query_id not in self._subscriptions:
                self._subscriptions[query_id] = []
            self._subscriptions[query_id].append(config)



    async def remove_subscription(self, query_id: str, consumer_id: str) -> None:
        """
        Removes a subscription for a given consumer.

        Args:
            query_id (str): The query ID for which the subscription is being removed.
            consumer_id (str): The ID of the consumer whose subscription is being removed.
        """
        async with self._lock:
            consumers = self._subscriptions.get(query_id)
            if consumers is None:
                return

            # TODO: optimize this
            self._subscriptions[query_id] = [
                consumer for consumer in consumers if consumer.id != consumer_id
            ]


    async def purge_subscriptions(self) -> None:
        """
        Purges all subscriptions.
        """
        async with self._lock:
            self._subscriptions.clear()
