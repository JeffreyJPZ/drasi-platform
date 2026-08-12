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

from utils.types import Operation

_OPERATION_FIELDS: tuple[Operation, ...] = ("added", "updated", "deleted")


def normalize_operations(operations: str | Operation | list[str | Operation] | None) -> list[Operation]:
    """
    Normalize operation input to a deduplicated, ordered list of operation fields.

    Args:
        operations (str | Operation | list[str | Operation] | None): The operations to normalize.
            If omitted or empty, all operations are included.
    
    Returns:
        list[Operation]: A deduplicated, ordered list of normalized operations.

    Raises:
        ValueError: If any of the operations are not supported or if the resulting list is empty.
    """
    # TODO: should empty or None really be considered as "all operations"?
    if not operations:
        return list(_OPERATION_FIELDS)

    raw_operations = [operations] if isinstance(operations, str) else list(operations)
    normalized_operations: list[Operation] = []

    for operation in raw_operations:
        normalized_operation = operation.strip().lower()
        try:
            normalized_operation = Operation(normalized_operation)
        except ValueError:
            raise ValueError(
                f"Unsupported operation '{operation}'. Expected one of: {', '.join(_OPERATION_FIELDS)}"
            )
        if normalized_operation not in normalized_operations:
            normalized_operations.append(normalized_operation)

    if not normalized_operations:
        raise ValueError("operations must not be empty")

    return normalized_operations
