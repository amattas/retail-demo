# Python Conventions

## Version & Environment

- **Python 3.10+** required
- Use virtual environments (`venv` or `uv`)
- Dependencies managed via `pyproject.toml`

## Formatting & Linting

| Tool | Purpose | Config |
|------|---------|--------|
| Black | Code formatting | `line-length = 88` |
| isort | Import sorting | `profile = "black"` |
| ruff | Linting | Replaces flake8, pylint |
| mypy | Type checking | `strict = true` |

## Type Hints

Required for all function signatures:

```python
from __future__ import annotations

from collections.abc import Sequence
from typing import TypeVar

T = TypeVar("T")

def first_or_none(items: Sequence[T]) -> T | None:
    """Return first item or None if empty."""
    return items[0] if items else None
```

Modern syntax (Python 3.10+):
- `list[str]` instead of `List[str]`
- `dict[str, int]` instead of `Dict[str, int]`
- `str | None` instead of `Optional[str]`
- `X | Y` instead of `Union[X, Y]`

## Data Structures

### Pydantic Models (External Data)
Use for validated data from external sources:

```python
from pydantic import BaseModel, Field

class ReceiptEvent(BaseModel):
    """Event emitted when a receipt is created."""

    receipt_id: str = Field(..., description="Unique receipt identifier")
    store_id: int = Field(..., ge=1)
    total: float = Field(..., ge=0)

    model_config = {"frozen": True}
```

### Dataclasses (Internal Data)
Use for internal data containers:

```python
from dataclasses import dataclass, field

@dataclass(frozen=True)
class ProcessingResult:
    """Result of processing an order."""

    success: bool
    message: str
    errors: list[str] = field(default_factory=list)
```

### TypedDict (Dictionary Schemas)
Use when dict structure is required:

```python
from typing import TypedDict

class OrderDict(TypedDict):
    order_id: str
    items: list[str]
    total: float
```

## Module Structure

Standard module layout:

```python
"""Module docstring explaining purpose.

This module handles order processing for the retail system.
"""
from __future__ import annotations

# Standard library
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

# Third-party
from pydantic import BaseModel

# Local imports
from .exceptions import OrderError

# Type-only imports (avoid circular dependencies)
if TYPE_CHECKING:
    from .customer import Customer

# Module-level logger
logger = logging.getLogger(__name__)

# Constants
MAX_ORDER_ITEMS = 100

# Public classes and functions
@dataclass
class Order:
    """Represents a customer order."""
    ...

def process_order(order: Order) -> ProcessingResult:
    """Process an order and return result."""
    ...

# Private helpers (underscore prefix)
def _validate_items(items: list[str]) -> bool:
    """Internal validation helper."""
    ...
```

## Modularity Guidelines

### File Size
- Keep modules under 300 lines
- Split large modules by responsibility:
  - `models.py` → `models/order.py`, `models/customer.py`

### Function Size
- Aim for < 20 lines per function
- Extract helper functions for complex logic

### Avoiding Circular Imports

```python
# Option 1: TYPE_CHECKING block
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .other_module import OtherClass

def process(item: "OtherClass") -> None:  # String annotation
    ...

# Option 2: Import inside function (last resort)
def process() -> None:
    from .other_module import OtherClass
    ...
```

## Error Handling

Define specific exceptions:

```python
# exceptions.py
class RetailError(Exception):
    """Base exception for retail operations."""

class OrderValidationError(RetailError):
    """Raised when order validation fails."""

class PaymentError(RetailError):
    """Raised when payment processing fails."""
```

Use them explicitly:

```python
def process_order(order: Order) -> Result:
    if not order.items:
        raise OrderValidationError("Order must have at least one item")

    try:
        payment_result = process_payment(order)
    except PaymentGatewayError as e:
        raise PaymentError(f"Payment failed: {e}") from e

    return Result.success(payment_result)
```

## Testing

Test file structure mirrors source:
- `src/retail/orders.py` → `tests/retail/test_orders.py`

```python
import pytest
from retail.orders import process_order, Order

class TestProcessOrder:
    """Tests for process_order function."""

    def test_valid_order_succeeds(self) -> None:
        """Valid orders should process successfully."""
        order = Order(items=["item1"], total=10.0)
        result = process_order(order)
        assert result.success

    def test_empty_order_raises_error(self) -> None:
        """Empty orders should raise validation error."""
        order = Order(items=[], total=0.0)
        with pytest.raises(OrderValidationError):
            process_order(order)
```
