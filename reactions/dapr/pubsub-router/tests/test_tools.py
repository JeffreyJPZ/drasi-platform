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

from app.mcp_server.tools import DrasiQueryToolSet
from app.subscription import SubscriptionRegistry
from app.utils.types import PubSubConsumerConfig, PubSubPayload, QueryConfig


class DummyMCP:
    def tool(self, fn):
        return fn


def _run(coro):
    return asyncio.run(coro)


def _make_toolset() -> DrasiQueryToolSet:
    reaction_config = {
        "query-1": QueryConfig(
            name="Query 1",
            description="A test query",
            added=PubSubPayload(task="added"),
            updated=PubSubPayload(task="updated"),
            deleted=PubSubPayload(task="deleted"),
        )
    }
    registry = SubscriptionRegistry(dapr_client=Mock(), use_state_store=False)
    return DrasiQueryToolSet(
        dapr_client=Mock(),
        mcp=DummyMCP(),
        pubsub_name="pubsub",
        subscription_registry=registry,
        reaction_config=reaction_config,
    )


def test_subscribe_normalizes_operations_and_upserts_existing_subscription() -> None:
    toolset = _make_toolset()

    _run(toolset.subscribe("query-1", "agent-1", "topic-a", operations="added"))
    _run(
        toolset.subscribe(
            "query-1",
            "agent-1",
            "topic-b",
            operations=["updated", "added", "updated"],
        )
    )

    subscription = _run(toolset._subscription_registry.get_subscription("query-1", "agent-1"))
    assert subscription is not None
    assert subscription.topic == "topic-b"
    assert subscription.added == PubSubPayload(task="added")
    assert subscription.updated == PubSubPayload(task="updated")
    assert subscription.deleted is None


def test_subscribe_none_maps_to_all_operations() -> None:
    toolset = _make_toolset()

    _run(toolset.subscribe("query-1", "agent-1", "topic-a", operations=None))

    subscription = _run(toolset._subscription_registry.get_subscription("query-1", "agent-1"))
    assert subscription is not None
    assert subscription.added == PubSubPayload(task="added")
    assert subscription.updated == PubSubPayload(task="updated")
    assert subscription.deleted == PubSubPayload(task="deleted")


def test_unsubscribe_clears_requested_operations_and_deletes_empty_subscription() -> None:
    toolset = _make_toolset()

    _run(toolset.subscribe("query-1", "agent-1", "topic-a", operations=None))
    _run(toolset.unsubscribe("query-1", "agent-1", operations=["added", "added"]))

    subscription = _run(toolset._subscription_registry.get_subscription("query-1", "agent-1"))
    assert subscription is not None
    assert subscription.added is None
    assert subscription.updated == PubSubPayload(task="updated")
    assert subscription.deleted == PubSubPayload(task="deleted")

    _run(toolset.unsubscribe("query-1", "agent-1", operations=None))
    assert _run(toolset._subscription_registry.get_subscription("query-1", "agent-1")) is None


def test_unsubscribe_is_idempotent_when_subscription_is_missing() -> None:
    toolset = _make_toolset()

    result = _run(toolset.unsubscribe("query-1", "agent-1", operations="deleted"))
    assert "successfully unsubscribed" in result
    assert _run(toolset._subscription_registry.get_subscription("query-1", "agent-1")) is None
