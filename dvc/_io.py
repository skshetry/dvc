import sys
from functools import cached_property
from typing import TextIO

from rich.console import Console


class Formatter:
    pass


class Printer:
    def __init__(
        self, inp: TextIO = None, out: TextIO = None, err: TextIO = None
    ) -> None:
        self.stdin = inp or sys.stdin
        self.stdout = out or sys.stdout
        self.stderr = err or sys.stderr

    @cached_property
    def error_console(self):
        return Console(file=self.stderr)

    @cached_property
    def console(self):
        return Console(file=self.stdout)

    def prompt(self, prompt, prompt_stream="stdout", **kwargs):
        console = (
            self.console if prompt_stream == "stdout" else self.error_console
        )
        return console.input(prompt, stream=self.stdin, **kwargs)

    def write(self):
        pass

    def success():
        pass

    def warn():
        pass

    def error():
        pass

    def write_raw():
        pass

    def format():
        pass
