"""Synthetic clickstream event generation and streaming into Fabric.

The :mod:`retail_setup.clickstream` package produces deterministic synthetic
clickstream events (``page_view``, ``product_view``, ``cart_add``, ``search``)
and pushes them to a Fabric Eventstream custom endpoint, which lands them in the
``clickstream_eventhouse`` Eventhouse. See :mod:`retail_setup.clickstream.generator`.
"""

from __future__ import annotations

from retail_setup.clickstream.generator import (
    EVENT_TYPES,
    GeneratorConfig,
    build_detail,
    generate_event,
    iter_events,
    load_customer_ids,
)

__all__ = [
    "EVENT_TYPES",
    "GeneratorConfig",
    "build_detail",
    "generate_event",
    "iter_events",
    "load_customer_ids",
]
