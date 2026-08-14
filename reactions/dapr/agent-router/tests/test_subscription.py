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

import asyncio
from unittest.mock import AsyncMock, Mock

from agent_router.subscription import SubscriptionRegistry
from agent_router.stores import InMemoryStateStore
from agent_router.utils.types import EventType, QuerySubscription, QuerySubscriptionState, StateConfig


def _run(coro):
    return asyncio.run(coro)


def _make_registry() -> SubscriptionRegistry:
    return SubscriptionRegistry(
        dapr_client=Mock(),
        state_config=StateConfig(state_store_name="test"),
    )


def test_upsert_subscription_replaces_existing_subscription() -> None:
    registry = _make_registry()

    _run(
        registry.upsert_subscription(
            "query-1",
            "agent-1",
            topic="topic-a",
            event_types=[EventType.ADDED, EventType.UPDATED],
        )
    )
    _run(
        registry.upsert_subscription(
            "query-1",
            "agent-1",
            topic="topic-b",
            event_types=[EventType.DELETED],
        )
    )

    subscription = _run(registry.get_subscription("query-1", "agent-1"))
    assert subscription is not None
    assert subscription.id == "agent-1"
    assert subscription.query_id == "query-1"
    assert subscription.topic == "topic-b"
    assert subscription.event_types == [EventType.DELETED]


def test_update_subscription_can_change_topic_and_event_types() -> None:
    registry = _make_registry()

    _run(
        registry.upsert_subscription(
            "query-1",
            "agent-1",
            topic="topic-a",
            event_types=[EventType.ADDED, EventType.UPDATED],
        )
    )

    _run(
        registry.update_subscription(
            "query-1",
            "agent-1",
            topic="topic-b",
        )
    )

    subscription = _run(registry.get_subscription("query-1", "agent-1"))
    assert subscription is not None
    assert subscription.topic == "topic-b"
    assert subscription.event_types == [EventType.ADDED, EventType.UPDATED]

    _run(
        registry.update_subscription(
            "query-1",
            "agent-1",
            event_types=[EventType.DELETED],
        )
    )

    subscription = _run(registry.get_subscription("query-1", "agent-1"))
    assert subscription is not None
    assert subscription.topic == "topic-b"
    assert subscription.event_types == [EventType.DELETED]


def test_delete_subscription_is_idempotent_when_missing() -> None:
    registry = _make_registry()

    _run(registry.delete_subscription("query-1", "agent-1"))
    assert _run(registry.get_subscription("query-1", "agent-1")) is None


def test_cache_is_used_after_write_through_upsert() -> None:
    registry = _make_registry()

    _run(
        registry.upsert_subscription(
            "query-1",
            "agent-1",
            topic="topic-a",
            event_types=[EventType.ADDED],
        )
    )

    registry._state_store.get_state = AsyncMock(side_effect=AssertionError("store read should not happen"))

    subscriptions = _run(registry.get_subscriptions("query-1"))
    assert len(subscriptions) == 1
    assert subscriptions[0].id == "agent-1"
    assert subscriptions[0].topic == "topic-a"

    subscription = _run(registry.get_subscription("query-1", "agent-1"))
    assert subscription is not None
    assert subscription.topic == "topic-a"


def test_get_subscription_populates_cache_on_miss() -> None:
    registry = _make_registry()
    registry._state_store = InMemoryStateStore(
        state_model_cls=QuerySubscriptionState,
        name="test",
    )

    _run(
        registry._state_store.save_state(
            "query-1",
            QuerySubscriptionState(
                root={
                    "agent-1": QuerySubscription(
                        id="agent-1",
                        query_id="query-1",
                        topic="topic-a",
                        event_types=[EventType.ADDED],
                    )
                },
            ),
        )
    )

    subscription = _run(registry.get_subscription("query-1", "agent-1"))
    assert subscription is not None
    assert subscription.topic == "topic-a"

    registry._state_store.get_state = AsyncMock(side_effect=AssertionError("store read should not happen"))

    subscriptions = _run(registry.get_subscriptions("query-1"))
    assert len(subscriptions) == 1
    assert subscriptions[0].id == "agent-1"


def test_delete_subscription_updates_cache() -> None:
    registry = _make_registry()

    _run(
        registry.upsert_subscription(
            "query-1",
            "agent-1",
            topic="topic-a",
            event_types=[EventType.ADDED, EventType.UPDATED],
        )
    )
    _run(registry.delete_subscription("query-1", "agent-1"))

    registry._state_store.get_state = AsyncMock(side_effect=AssertionError("store read should not happen"))

    assert _run(registry.get_subscription("query-1", "agent-1")) is None
    assert _run(registry.get_subscriptions("query-1")) == []
