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
from typing import Any

from stores.base import StateStore, TState


class InMemoryStateStore(StateStore[TState]):
    """
    Simple in-memory key-value state store for testing and development purposes.
    """

    def __init__(
        self,
        state_model_cls: type[TState],
        state_key_prefix: str | None = None,
        name: str | None = None,
    ) -> None:
        """
        Initialize an InMemoryStateStore instance.

        Args:
            state_model_cls (type[TState]): The state model class used for validation.
            state_key_prefix (str | None): Optional prefix for state keys.
            name (str | None): Optional state store name. Resolves to "drasi-pubsub-router-store" if omitted.
        """
        super().__init__(state_model_cls=state_model_cls, state_key_prefix=state_key_prefix, name=name)

        self._lock = asyncio.Lock()
        self._store: dict[str, Any] = {}


    async def get_state(self, key: str) -> TState:
        """
        Retrieve the state associated with the given key.

        Args:
            key (str): The key for which to retrieve the state.

        Returns:
            TState: A copy of the state associated with the key, or a default instance if not found.
        """
        state_key = self._normalize_key(key)

        async with self._lock:
            state = self._store.get(state_key, None)
            if state is None:
                return self._default_state_model_factory()
            return self._state_model_cls.model_validate(state) 


    async def save_state(self, key: str, value: TState) -> None:
        """
        Save the state associated with the given key.

        Args:
            key (str): The key for the state.
            value (TState): The state to be saved.
        """
        state_key = self._normalize_key(key)

        async with self._lock:
            value = value.model_dump(mode="json")
            self._store[state_key] = value


    async def purge_state(self, key: str) -> None:
        """
        Delete the state associated with the given key.

        Args:
            key (str): The key for which to delete the state.
        """
        state_key = self._normalize_key(key)

        async with self._lock:
            if state_key in self._store:
                self._store.pop(state_key, None)
