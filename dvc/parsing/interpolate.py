import re
from typing import Match
from .parser import Expression, parser

KEYCRE = re.compile(
    r"""
    (?<!\\)                   # escape \${} or ${{}}
    \$                        # starts with $
    (?:({{)|({))              # either starts with double braces or single
    ([\w._ \\/-]*?)           # match every char, attr access through "."
    (?(1)}})(?(2)})           # end with same kinds of braces it opened with
""",
    re.VERBOSE,
)


def _is_interpolated_string(val):
    return any(
        any(isinstance(i, Expression) for i in t)
        for t, _, _ in parser.scanString(val)
    )


def _is_exact_string(src: str):
    tokens = [t for toks, _, _ in parser.scanString(src) for t in toks]
    return len(tokens) == 1 and f"${{tokens[0]}}" == src


def _get_parts(match: Match):
    return parser.scanString()
