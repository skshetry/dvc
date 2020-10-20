import re
from collections.abc import Mapping

from funcy import rpartial

from dvc.parsing.context import Context, String, Value

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

UNWRAP_DEFAULT = False


def _get_matches(template):
    return list(KEYCRE.finditer(template))


def _resolve_value(match, context: Context):
    _, _, inner = match.groups()
    value = context.select(inner)
    return value


def _unwrap(value):
    if isinstance(value, (Value, String)):
        return value.value
    return value


def _resolve_str(src: str, context, unwrap=UNWRAP_DEFAULT):
    matches = _get_matches(src)
    if len(matches) == 1 and src == matches[0].group(0):
        # replace "${enabled}", if `enabled` is a boolean, with it's actual
        # value rather than it's string counterparts.
        value = _resolve_value(matches[0], context)
    else:
        value = String(src, matches, context)
    return _unwrap(value) if unwrap else value


def resolve(src, context, unwrap=UNWRAP_DEFAULT):
    Seq = (list, tuple, set)

    apply_value = rpartial(resolve, context, unwrap=unwrap)
    if isinstance(src, Mapping):
        return {key: apply_value(value) for key, value in src.items()}
    elif isinstance(src, Seq):
        return type(src)(map(apply_value, src))
    elif isinstance(src, str):
        return _resolve_str(src, context, unwrap=unwrap)
    return src
