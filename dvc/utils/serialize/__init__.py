from collections import defaultdict
from dvc.tree.base import BaseTree
from os import PathLike
from typing import (
    DefaultDict,
    ContextManager,
    Optional,
    Protocol,
    Union,
)

from ._common import *  # noqa, pylint: disable=wildcard-import
from ._json import *  # noqa, pylint: disable=wildcard-import
from ._py import *  # noqa, pylint: disable=wildcard-import
from ._toml import *  # noqa, pylint: disable=wildcard-import
from ._yaml import *  # noqa, pylint: disable=wildcard-import


Path = Union[str, bytes, PathLike]


class Loader(Protocol):
    def __call__(self, path: Path, tree: Optional[BaseTree]) -> dict:
        ...


class Modifier(Protocol):
    def __call__(
        self, path: Path, tree: Optional[BaseTree]
    ) -> ContextManager[dict]:
        ...


LOADERS: DefaultDict[str, Loader] = defaultdict(
    lambda: load_yaml
)  # noqa: F405
LOADERS.update(
    {".toml": load_toml, ".json": load_json, ".py": load_py}  # noqa: F405
)


MODIFIERS: DefaultDict[str, Modifier] = defaultdict(
    lambda: modify_yaml
)  # noqa: F405
MODIFIERS.update(
    {
        ".toml": modify_toml,  # noqa: F405
        ".json": modify_json,  # noqa: F405
        ".py": modify_py,  # noqa: F405
    }
)
