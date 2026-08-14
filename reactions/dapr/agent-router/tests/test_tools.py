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

from agent_router.mcp_server.tools import AgentRouterToolset
from agent_router.subscription import SubscriptionRegistry
from agent_router.utils.types import EventType, QueryConfig, StateConfig


class DummyMCP:
    def tool(self, fn):
        return fn


def _run(coro):
    return asyncio.run(coro)


def _make_toolset() -> AgentRouterToolset:
    reaction_config = {
        "query-1": QueryConfig(
            title="Query 1",
            description="A test query",
        )
    }
    registry = SubscriptionRegistry(
        dapr_client=Mock(),
        state_config=StateConfig(store_name="test"),
    )
    return AgentRouterToolset(
        mcp=DummyMCP(),
        subscription_registry=registry,
        query_configs=reaction_config,
    )


def test_subscribe_normalizes_operations_and_upserts_existing_subscription() -> None:
    toolset = _make_toolset()

    _run(toolset.subscribe("query-1", "agent-1", "topic-a", event_types=[EventType.ADDED]))
    _run(
        toolset.subscribe(
            "query-1",
            "agent-1",
            "topic-b",
            event_types=[EventType.UPDATED, EventType.ADDED, EventType.UPDATED],
        )
    )

    subscription = _run(toolset._subscription_registry.get_subscription("query-1", "agent-1"))
    assert subscription is not None
    assert subscription.query_id == "query-1"
    assert subscription.topic == "topic-b"
    assert subscription.event_types == [EventType.UPDATED, EventType.ADDED]


def test_subscribe_rejects_empty_event_types() -> None:
    toolset = _make_toolset()

    try:
        _run(toolset.subscribe("query-1", "agent-1", "topic-a", event_types=[]))
    except Exception:
        pass
    else:
        raise AssertionError("subscribe should reject an empty event_types list")


def test_unsubscribe_clears_requested_operations_and_deletes_empty_subscription() -> None:
    toolset = _make_toolset()

    _run(toolset.subscribe("query-1", "agent-1", "topic-a", event_types=[EventType.ADDED, EventType.UPDATED, EventType.DELETED]))
    _run(toolset.unsubscribe("query-1", "agent-1", event_types=[EventType.ADDED, EventType.ADDED]))

    subscription = _run(toolset._subscription_registry.get_subscription("query-1", "agent-1"))
    assert subscription is not None
    assert subscription.event_types == [EventType.UPDATED, EventType.DELETED]

    _run(toolset.unsubscribe("query-1", "agent-1", event_types=[EventType.UPDATED, EventType.DELETED]))
    assert _run(toolset._subscription_registry.get_subscription("query-1", "agent-1")) is None


def test_unsubscribe_is_idempotent_when_subscription_is_missing() -> None:
    toolset = _make_toolset()

    result = _run(toolset.unsubscribe("query-1", "agent-1", event_types=[EventType.DELETED]))
    assert "successfully unsubscribed" in result
    assert _run(toolset._subscription_registry.get_subscription("query-1", "agent-1")) is None
