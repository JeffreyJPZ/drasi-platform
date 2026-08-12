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
from typing import Any

from dapr.clients import DaprClient

from stores import DaprStateStore, InMemoryStateStore, StateStore
from utils.types import Operation, PubSubConsumerConfig, QuerySubscriptionState, StateStoreConfig
from utils.validation import normalize_operations

logger = logging.getLogger(__name__)


class SubscriptionRegistry():
    """
    Subscription management for the Dapr Agent Router.
    """

    def __init__(
        self,
        dapr_client: DaprClient,
        state_store_config: StateStoreConfig,
    ) -> None:
        """
        Initialize a SubscriptionRegistry instance.

        Args:
            dapr_client (DaprClient): Injected Dapr client.
            state_store_config (StateStoreConfig): Configuration for the state store backend.
        """
        self._state_store = self._make_subscription_state_store(
            dapr_client=dapr_client,
            state_model_cls=QuerySubscriptionState,
            state_store_config=state_store_config
        )


    async def get_subscription(self, query_id: str, consumer_id: str) -> PubSubConsumerConfig | None:
        """
        Get a subscription for a given query ID and consumer ID.

        Args:
            query_id (str): The query ID for which the subscription is being retrieved.
            consumer_id (str): The consumer ID whose subscription is being retrieved.
        """
        state = await self._state_store.get_state(query_id)
        subscriptions = state.root if state is not None else {}

        # TODO: should not be keyed on consumer ID only
        # compose with principal (e.g. API key ID, OAuth sub, SPIFFE ID) in authenticated mode
        # or source (e.g. registered workflow ID) in unauthenticated mode?
        return subscriptions.get(consumer_id, None)


    async def get_subscriptions(
        self,
        query_id: str,
        operations: str | Operation | list[str | Operation] | None = None
    ) -> list[PubSubConsumerConfig]:
        """
        Get all subscriptions for a given query ID and match any of the given operations (if provided).

        Args:
            query_id (str): The query ID for which the subscription is being retrieved.
            operations (str | list[str] | None): The operations to filter by.
                If omitted or empty, all operations are included.
        """
        normalized_operations = normalize_operations(operations)
        state = await self._state_store.get_state(query_id)
        subscriptions = state.root if state is not None else {}
        subscriptions_list = list(subscriptions.values())

        if not normalized_operations:
            return subscriptions_list
        # TODO: fine-grained subscriptions are dependent on operations matching pubsub template field names
        # maybe make this a helper instead to abstract it
        return [sub for sub in subscriptions_list if any(getattr(sub, op) is not None for op in normalized_operations)]


    async def upsert_subscription(
        self,
        query_id: str,
        consumer_id: str,  # TODO: use a unified name instead of having agent ID and consumer ID
        config: PubSubConsumerConfig,
    ) -> None:
        """
        Create or update a subscription for a given query ID and consumer ID.

        Args:
            query_id (str): The query ID for which the subscription is being added.
            consumer_id (str): The consumer ID whose subscription is being added.
            config (PubSubConsumerConfig): The configuration for the subscription containing a consumer ID.
        """
        state = await self._state_store.get_state(query_id)
        subscriptions = state.root if state is not None else {}

        existing = subscriptions.get(consumer_id, None)
        config = config.model_copy(update={"id": consumer_id})

        # TODO: this fully overwrites the existing subscription, may need more granular updates
        if existing is None:
            subscriptions[consumer_id] = config
        else:
            subscriptions[consumer_id] = existing.model_copy(
                update=config.model_dump(exclude_unset=True)
            )

        await self._state_store.save_state(query_id, state)     


    async def update_subscription(
        self,
        query_id: str,
        consumer_id: str,
        fields: dict[str, Any],
    ) -> None:
        """
        Update an existing subscription for a given consumer ID.

        Args:
            query_id (str): The query ID for which the subscription is being updated.
            consumer_id (str): The consumer ID whose subscription is being updated.
            fields (dict[str, Any]): Partial subscription data to merge into the existing subscription.
        """
        state = await self._state_store.get_state(query_id)
        subscriptions = state.root if state is not None else {}
        existing_sub = subscriptions.get(consumer_id, None)

        if existing_sub is None:
            return

        updated_sub = existing_sub.model_copy(update=fields)

        # If all fields are None, remove the subscription entirely
        if all(
            getattr(updated_sub, field) is None for field in ("added", "updated", "deleted")  # TODO: make this an enum
        ):
            subscriptions.pop(consumer_id, None)
            if subscriptions:
                await self._state_store.save_state(query_id, state)
            else:
                await self._state_store.purge_state(query_id)
            return

        subscriptions[consumer_id] = updated_sub
        await self._state_store.save_state(query_id, state)


    async def delete_subscription(self, query_id: str, consumer_id: str) -> None:
        """
        Delete a subscription for a given consumer ID.

        Args:
            query_id (str): The query ID for which the subscription is being deleted.
            consumer_id (str): The consumer ID whose subscription is being deleted.
        """
        state = await self._state_store.get_state(query_id)
        subscriptions = state.root if state is not None else {}

        subscriptions.pop(consumer_id, None)

        if subscriptions:
            await self._state_store.save_state(query_id, state)
        else:
            await self._state_store.purge_state(query_id)


    def _make_subscription_state_store(
        self,
        *,
        dapr_client: DaprClient,
        state_model_cls: type[QuerySubscriptionState],
        state_store_config: StateStoreConfig,
    ) -> StateStore[QuerySubscriptionState]:
        """
        Create a state store instance based on the configuration.

        Args:
            dapr_client (DaprClient): Injected Dapr client.
            state_model_cls (type[QuerySubscriptionState]): The state model class.
            state_store_config (StateStoreConfig): Configuration for the state store backend.
    
        Returns:
            StateStore[QuerySubscriptionState]: The state store instance.

        Raises:
            ValueError: If the state store type is unsupported.
        """
        match state_store_config.type:
            case "dapr":
                return DaprStateStore[state_model_cls](
                    dapr_client=dapr_client,
                    state_model_cls=state_model_cls,
                    store_name=state_store_config.store_name,
                )
            case "in-memory":
                return InMemoryStateStore[state_model_cls](
                    state_model_cls=state_model_cls,
                )
            case _:
                raise ValueError(f"Unsupported state store type: {state_store_config.type}")
