"""
Utility functions for Fabric notebook environment configuration.

These helpers are used in notebooks to enforce required configuration
and provide clear error messages when environment variables are missing.
"""

import os


def get_required_env(var_name: str, example: str | None = None) -> str:
    """
    Get a required environment variable or raise a clear error.

    This helper function is used in notebooks to enforce required configuration.

    Args:
        var_name: Name of the environment variable
        example: Optional example value to show in error message

    Returns:
        The environment variable value

    Raises:
        ValueError: If environment variable is not set or empty
    """
    value = os.environ.get(var_name, "").strip()

    if not value:
        example_text = f" (e.g., '{example}')" if example else ""
        raise ValueError(
            f"Environment variable '{var_name}' is required but not set.\n"
            f"Please set it before running this notebook{example_text}.\n"
            f"Example: export {var_name}=<your_value>"
        )

    return value
