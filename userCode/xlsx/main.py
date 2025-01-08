# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import click
import debugpy
from lib import parse_xlsx
import os
import pytest
from pathlib import Path


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("file", type=click.Path(exists=True))
def load(ctx, file):
    """Load and process an xlsx file, parsing and sending to Frost."""
    path = Path(file)
    parsed = parse_xlsx(path)
    sta = parsed.to_sta()
    parsed.send_to_frost(sta)
    click.echo(f"Finished uploading {path} to FROST")


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("pytest_args", nargs=-1, type=click.UNPROCESSED)
def test(ctx, pytest_args):
    """Run all pytest tests associated with this module. Pass in additional arguments to pytest if needed."""
    dir_path = os.path.dirname(os.path.realpath(__file__))
    test_dir = os.path.join(dir_path, "tests")
    pytest.main([test_dir, "-vvvx", *pytest_args])


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("pytest_args", nargs=-1, type=click.UNPROCESSED)
def test_debug(ctx, pytest_args):
    """Run tests with debugpy for debugging. Requires an external debugger to connect to the port"""
    debugpy.listen(("0.0.0.0", 5678))
    print(
        "Waiting for debugger attach... If you are using vscode, use the Attach Debugger configuration in this repo"
    )
    debugpy.wait_for_client()
    print("Debugger attached.")
    dir_path = os.path.dirname(os.path.realpath(__file__))
    test_dir = os.path.join(dir_path, "tests")
    pytest.main([test_dir, "-vvvx", *pytest_args])


@click.group()
def xlsx():
    """Station metadata management via xlsx files which represent a STA data model."""
    pass


xlsx.add_command(test)
xlsx.add_command(test_debug)
xlsx.add_command(load)

if __name__ == "__main__":
    xlsx()
