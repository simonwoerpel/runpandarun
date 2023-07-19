from pathlib import Path
from typing import Optional

import typer
from typing_extensions import Annotated

from .playbook import Playbook

cli = typer.Typer()


@cli.command()
def run(
    path: Path,
    in_uri: Annotated[
        Optional[str], typer.Option("-i", help="Input uri, use `-` for stdin")
    ] = None,
    out_uri: Annotated[
        Optional[str], typer.Option("-o", help="Output uri, use `-` for stdout")
    ] = None,
    read_handler: Annotated[
        Optional[str], typer.Option("-rh", help="Read handler for pandas")
    ] = None,
    write_handler: Annotated[
        Optional[str], typer.Option("-wh", help="Write handler for pandas")
    ] = None,
):
    if not path.exists() or not path.is_file():
        raise ValueError("Invalid path: `%s`" % path)
    play = Playbook.from_yaml(path)
    if in_uri is not None:
        play.read.uri = in_uri
    if read_handler is not None:
        play.read.handler = read_handler
    if out_uri is not None:
        play.write.uri = out_uri
    if write_handler is not None:
        play.write.handler = write_handler
    play.run(write=True)
