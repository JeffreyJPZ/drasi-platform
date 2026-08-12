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
from unittest.mock import Mock

from app.subscription import SubscriptionRegistry
from app.utils.types import PubSubConsumerConfig, PubSubPayload


def _run(coro):
    return asyncio.run(coro)


def _make_registry() -> SubscriptionRegistry:
    return SubscriptionRegistry(dapr_client=Mock(), use_state_store=False)


def test_upsert_subscription_merges_fields_without_dropping_existing_data() -> None:
    registry = _make_registry()

    _run(
        registry.upsert_subscription(
            "query-1",
            "agent-1",
            PubSubConsumerConfig(
                id="agent-1",
                topic="topic-a",
                added=PubSubPayload(task="added"),
            ),
        )
    )
    _run(
        registry.upsert_subscription(
            "query-1",
            "agent-1",
            PubSubConsumerConfig(
                id="agent-1",
                topic="topic-b",
                updated=PubSubPayload(task="updated"),
            ),
        )
    )

    subscription = _run(registry.get_subscription("query-1", "agent-1"))
    assert subscription is not None
    assert subscription.topic == "topic-b"
    assert subscription.added == PubSubPayload(task="added")
    assert subscription.updated == PubSubPayload(task="updated")
    assert subscription.deleted is None


def test_update_subscription_can_clear_fields_and_delete_when_empty() -> None:
    registry = _make_registry()

    _run(
        registry.upsert_subscription(
            "query-1",
            "agent-1",
            PubSubConsumerConfig(
                id="agent-1",
                topic="topic-a",
                added=PubSubPayload(task="added"),
                updated=PubSubPayload(task="updated"),
            ),
        )
    )

    updated = _run(
        registry.update_subscription(
            "query-1",
            "agent-1",
            {
                "added": None,
            },
        )
    )
    assert updated is None
    assert _run(registry.get_subscription("query-1", "agent-1")) is not None

    deleted = _run(
        registry.update_subscription(
            "query-1",
            "agent-1",
            {
                "updated": None,
            },
        )
    )
    assert deleted is None
    assert _run(registry.get_subscription("query-1", "agent-1")) is None


def test_delete_subscription_is_idempotent_when_missing() -> None:
    registry = _make_registry()

    _run(registry.delete_subscription("query-1", "agent-1"))
    assert _run(registry.get_subscription("query-1", "agent-1")) is None
