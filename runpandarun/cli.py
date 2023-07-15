from pathlib import Path
from typing import Optional

import typer
from typing_extensions import Annotated

from .playbook import Playbook

cli = typer.Typer()


@cli.command()
def run(
    path: Path,
    in_uri: Annotated[Optional[str], typer.Option("-i")] = None,
    out_uri: Annotated[Optional[str], typer.Option("-o")] = None,
):
    if not path.exists() or not path.is_file():
        raise ValueError("Invalid path: `%s`" % path)
    play = Playbook.from_yaml(path)
    if in_uri is not None:
        play.read.uri = in_uri
    if out_uri is not None:
        play.write.uri = out_uri
    play.run(write=True)
