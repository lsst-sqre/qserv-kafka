"""Commandline interface for qserv-kafka."""

import click
from safir.asyncio import run_with_asyncio
from safir.click import display_help

from .periodic_metrics import publish_arq_metrics


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(message="%(version)s")
def main() -> None:
    """qserv-kafka commandline interface."""


@main.command()
@click.argument("topic", default=None, required=False, nargs=1)
@click.argument("subtopic", default=None, required=False, nargs=1)
@click.pass_context
def help(ctx: click.Context, topic: str | None, subtopic: str | None) -> None:
    """Show help for any command."""
    display_help(main, ctx, topic, subtopic)


@main.command()
@run_with_asyncio
async def publish_periodic_metrics() -> None:
    """Publish metrics, meant to be executed periodically."""
    await publish_arq_metrics()
