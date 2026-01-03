# Python Coding Standards (PEP 8 & PEP 20)

## Line Length & Formatting (PEP 8)

- **Maximum line length: 88 characters** (Black formatter default)
- Use 4 spaces for indentation (never tabs)
- Two blank lines before top-level function/class definitions
- One blank line between methods in a class
- No trailing whitespace
- Use Black for auto-formatting
- Use isort for import sorting

## Imports (PEP 8)

```python
# 1. Standard library
import logging
import os
from datetime import datetime
from typing import TYPE_CHECKING

# 2. Third-party
from pydantic import BaseModel

# 3. Local
from .models import MyModel
```

- Group imports in order: standard library, third-party, local
- One import per line for clarity
- Use absolute imports; avoid wildcard imports (`from x import *`)
- Sort alphabetically within groups
- Use `TYPE_CHECKING` for import-only type hints to avoid circular imports

## The Zen of Python (PEP 20)

| Principle | Practical Meaning |
|-----------|-------------------|
| **Beautiful is better than ugly** | Write clean, readable code |
| **Explicit is better than implicit** | Be clear about intent; no magic |
| **Simple is better than complex** | Start simple, add complexity only when needed |
| **Complex is better than complicated** | If complexity is needed, keep it organized |
| **Flat is better than nested** | Limit nesting to 3-4 levels max |
| **Sparse is better than dense** | Use whitespace for readability |
| **Readability counts** | Code is read more than written |
| **Errors should never pass silently** | Handle or propagate, never swallow |
| **If the implementation is hard to explain, it's a bad idea** | Simplify until explainable |

## Structure & Modularity

### Functions
- **Single Responsibility**: Each function does one thing well
- **Size**: Aim for < 20 lines per function; split if larger
- **Parameters**: 3-4 max; use dataclasses/Pydantic for more
- **Early returns**: Use guard clauses to reduce nesting

```python
# Good: Early return
def process_order(order: Order) -> Result:
    if not order.is_valid:
        return Result.invalid("Order validation failed")

    if order.is_cancelled:
        return Result.cancelled()

    return Result.success(order.process())

# Bad: Deep nesting
def process_order(order: Order) -> Result:
    if order.is_valid:
        if not order.is_cancelled:
            return Result.success(order.process())
        else:
            return Result.cancelled()
    else:
        return Result.invalid("Order validation failed")
```

### Modules
- **Size**: Keep modules under 300 lines; split if larger
- **Focus**: One module = one concept or domain area
- **Classes**: One class per file for complex classes
- **Avoid circular imports**: Use dependency injection or `TYPE_CHECKING`

## Naming Conventions (PEP 8)

| Element | Convention | Example |
|---------|------------|---------|
| Functions | `snake_case` | `calculate_total_price()` |
| Variables | `snake_case` | `order_count` |
| Classes | `PascalCase` | `OrderProcessor` |
| Constants | `UPPER_SNAKE_CASE` | `MAX_RETRY_COUNT` |
| Modules | `snake_case` | `order_processing.py` |
| Private | `_prefix` | `_internal_helper()` |

- Use descriptive names: `calculate_total_price()` not `calc()`
- Boolean variables/functions: `is_valid`, `has_items`, `can_process`

## Type Hints

- Required for all function signatures
- Use `from __future__ import annotations` for forward references
- Prefer modern syntax: `list[str]` over `List[str]` (Python 3.9+)

```python
from __future__ import annotations

def process_items(
    items: list[str],
    options: dict[str, int] | None = None,
) -> tuple[list[str], int]:
    """Process items with optional configuration."""
    ...
```

## Docstrings (Google Style)

```python
def calculate_discount(
    price: float,
    discount_percent: float,
    min_price: float = 0.0,
) -> float:
    """Calculate discounted price with minimum floor.

    Args:
        price: Original price before discount.
        discount_percent: Discount as percentage (0-100).
        min_price: Minimum price floor after discount.

    Returns:
        Final price after applying discount, not less than min_price.

    Raises:
        ValueError: If discount_percent is not between 0 and 100.

    Example:
        >>> calculate_discount(100.0, 20.0)
        80.0
    """
    if not 0 <= discount_percent <= 100:
        raise ValueError(f"Invalid discount: {discount_percent}")

    discounted = price * (1 - discount_percent / 100)
    return max(discounted, min_price)
```

## Error Handling

```python
# Good: Specific exception with context
try:
    result = process_order(order)
except OrderValidationError as e:
    logger.error("Order validation failed", extra={"order_id": order.id, "error": str(e)})
    raise
except PaymentError as e:
    logger.error("Payment processing failed", extra={"order_id": order.id})
    return OrderResult.payment_failed(str(e))

# Bad: Bare except or generic handling
try:
    result = process_order(order)
except:  # Never do this
    pass
```

## Logging

```python
import logging

logger = logging.getLogger(__name__)

# Structured logging with context
logger.info(
    "Order processed successfully",
    extra={
        "order_id": order.id,
        "customer_id": order.customer_id,
        "total": order.total,
    }
)

# Appropriate log levels
logger.debug("Detailed debugging info")
logger.info("Normal operation events")
logger.warning("Something unexpected but handled")
logger.error("Error that needs attention")
```
