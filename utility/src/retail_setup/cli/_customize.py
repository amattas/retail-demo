"""CLI flow shared by `retail-setup customize` and `configure` for custom business layouts."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import typer

from retail_setup import interaction
from retail_setup.customization.generator import (
    ChatFactory,
    CustomizationError,
    copilot_chat,
    generate_spec,
)
from retail_setup.customization.preview import preview_lines
from retail_setup.customization.spec import (
    CustomizationSpec,
    slugify,
    validate_business_name,
)
from retail_setup.customization.store import (
    customization_dir,
    load_spec,
    materialize,
    save_spec,
)

CUSTOM_CHOICE = "custom"


def load_spec_file(path: Path) -> CustomizationSpec:
    """Load a spec from an explicit file (`--from-spec`), validating it fully."""

    try:
        return CustomizationSpec.model_validate_json(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise CustomizationError(f"Invalid customization spec {path}: {exc}") from exc


def ensure_customization(
    repo_root: Path,
    env: str,
    business_name: str | None,
    *,
    regenerate: bool = False,
    from_spec: Path | None = None,
    model: str | None = None,
    assume_yes: bool = False,
    echo: Callable[[str], None] = typer.echo,
    chat_factory: ChatFactory = copilot_chat,
) -> CustomizationSpec:
    """Return the env's accepted spec, generating it with Copilot when needed.

    A saved spec for the same business is reused unless ``regenerate`` is set.
    Interactive operators review a preview and may regenerate before accepting.
    Raises CustomizationError when no spec is accepted.
    """

    directory = customization_dir(repo_root, env)
    if from_spec is not None:
        spec = load_spec_file(from_spec)
        return _accept(spec, directory, echo)

    saved = load_spec(directory)
    if business_name is None:
        if saved is None:
            raise CustomizationError(
                f"No custom layout is saved for {env!r}; pass --business NAME."
            )
        business_name = saved.business_name
    business_name = validate_business_name(business_name)
    if saved is not None and not regenerate and saved.slug == slugify(business_name):
        echo(f"Using the saved {saved.display_name} layout ({directory / 'customization.json'}).")
        return _accept(saved, directory, echo, announce=False)

    interactive = interaction.is_interactive() and not assume_yes
    while True:
        echo(f"Generating a custom layout for {business_name!r} with GitHub Copilot...")
        spec = generate_spec(
            business_name, model=model, chat_factory=chat_factory, progress=echo
        )
        detail = "\n".join(preview_lines(spec))
        if not interactive:
            echo(detail)
            return _accept(spec, directory, echo)
        answer = interaction.choose(
            "Use this layout?",
            ["accept", "regenerate", "cancel"],
            default="accept",
            detail=detail,
            title=f"{spec.display_name} layout",
        )
        if answer == "accept":
            return _accept(spec, directory, echo)
        if answer == "cancel":
            raise CustomizationError("Custom layout generation was cancelled.")


def _accept(
    spec: CustomizationSpec, directory: Path, echo: Callable[[str], None], *, announce: bool = True
) -> CustomizationSpec:
    path = save_spec(spec, directory)
    root = materialize(spec, directory)
    if announce:
        echo(f"Saved {path}")
        echo(f"Dictionaries: {root / spec.store_type}")
    return spec
