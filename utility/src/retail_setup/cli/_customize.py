"""CLI flow shared by `retail-setup customize` and `configure` for custom business layouts."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import typer

from retail_setup import interaction
from retail_setup.customization.generator import (
    ChatFactory,
    CustomizationError,
    choose_logo,
    copilot_chat,
    generate_spec,
)
from retail_setup.customization.logo import Logo, LogoError, logo_from_file, restore_logo, save_logo
from retail_setup.customization.preview import preview_lines
from retail_setup.customization.sdk import ensure_sdk
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
    logo_file: Path | None = None,
    no_logo: bool = False,
    echo: Callable[[str], None] = typer.echo,
    chat_factory: ChatFactory = copilot_chat,
) -> CustomizationSpec:
    """Return the env's accepted spec, generating it with Copilot when needed.

    A saved spec for the same business is reused unless ``regenerate`` is set.
    Interactive operators review a preview and may regenerate before accepting.
    Copilot also picks the business's logo from Wikimedia Commons once;
    ``logo_file`` supplies one instead and ``no_logo`` removes it.
    Raises CustomizationError when no spec is accepted.
    """

    directory = customization_dir(repo_root, env)
    overridden, override = _logo_override(logo_file, no_logo)
    if from_spec is not None:
        spec = load_spec_file(from_spec)
        logo = override if overridden else _restore_logo(spec, directory)
        return _accept(spec, logo, directory, echo)

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
        logo = override if overridden else _restore_logo(saved, directory)
        return _accept(saved, logo, directory, echo, announce=overridden)

    interactive = interaction.is_interactive() and not assume_yes
    if chat_factory is copilot_chat:

        def confirm(note: str) -> bool:
            if not interactive:
                # --business/--yes is the operator's explicit opt-in.
                echo(note)
                return True
            return interaction.confirm(
                "Install the GitHub Copilot SDK now?", default=True, detail=note
            )

        ensure_sdk(repo_root, confirm=confirm, progress=echo)
    logo, chosen = override, overridden
    while True:
        echo(f"Generating a custom layout for {business_name!r} with GitHub Copilot...")
        spec = generate_spec(business_name, model=model, chat_factory=chat_factory, progress=echo)
        if not chosen:
            logo = choose_logo(business_name, model=model, chat_factory=chat_factory, progress=echo)
            chosen = True
        detail = "\n".join(preview_lines(spec, logo))
        if not interactive:
            echo(detail)
            return _accept(spec, logo, directory, echo)
        answer = interaction.choose(
            "Use this layout?",
            ["accept", "regenerate", "cancel"],
            default="accept",
            detail=detail,
            title=f"{spec.display_name} layout",
        )
        if answer == "accept":
            return _accept(spec, logo, directory, echo)
        if answer == "cancel":
            raise CustomizationError("Custom layout generation was cancelled.")


def _logo_override(logo_file: Path | None, no_logo: bool) -> tuple[bool, Logo | None]:
    """``(True, logo)`` when the operator chose the logo, else ``(False, None)``."""

    if logo_file is not None and no_logo:
        raise CustomizationError("Use either --logo-file or --no-logo, not both.")
    if no_logo:
        return True, None
    if logo_file is None:
        return False, None
    try:
        return True, logo_from_file(logo_file)
    except (LogoError, OSError) as exc:
        raise CustomizationError(f"Cannot use logo {logo_file}: {exc}") from exc


def _restore_logo(spec: CustomizationSpec, directory: Path) -> Logo | None:
    try:
        return restore_logo(directory, spec.logo)
    except (LogoError, OSError) as exc:
        raise CustomizationError(
            f"{exc}\nPass --logo-file PATH to supply the logo, or --no-logo to drop it."
        ) from exc


def _accept(
    spec: CustomizationSpec,
    logo: Logo | None,
    directory: Path,
    echo: Callable[[str], None],
    *,
    announce: bool = True,
) -> CustomizationSpec:
    spec = spec.model_copy(update={"logo": None if logo is None else logo.info})
    path = save_spec(spec, directory)
    save_logo(directory, logo)
    root = materialize(spec, directory)
    if announce:
        echo(f"Saved {path}")
        echo(f"Dictionaries: {root / spec.store_type}")
        echo(f"Logo: {logo.info.title}" if logo else "Logo: none (titles show the name only)")
    return spec
