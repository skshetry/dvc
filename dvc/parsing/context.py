from abc import ABC
from dvc.tree.base import BaseTree
import os
from collections import defaultdict
from collections.abc import Mapping, MutableMapping, MutableSequence
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field, replace
from typing import (
    Any,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Protocol,
    Sequence,
    Sized,
    TypeVar,
    Union,
    overload,
)

from funcy import identity

from dvc.utils.serialize import LOADERS


def _merge(into: MutableMapping, update: Mapping, overwrite: bool):
    for key, val in update.items():
        if isinstance(into.get(key), Mapping) and isinstance(val, Mapping):
            _merge(into[key], val, overwrite)
        else:
            if key in into and not overwrite:
                raise ValueError(
                    f"Cannot overwrite as key {key} already exists in {into}"
                )
            into[key] = val


@dataclass
class Meta:
    source: Optional[str] = None
    dpaths: List[str] = field(default_factory=list)

    @staticmethod
    def update_path(meta: "Meta", path: Union[str, int]):
        dpaths = meta.dpaths[:] + [str(path)]
        return replace(meta, dpaths=dpaths)

    def __str__(self):
        string = self.source or "<local>:"
        string += self.path()
        return string

    def path(self):
        return ".".join(self.dpaths)


def _default_meta():
    return Meta(source=None)


class Node:
    def get_sources(self):
        raise NotImplementedError


@dataclass
class Value(Node):
    value: Any
    meta: Meta = field(
        compare=False, default_factory=_default_meta, repr=False
    )

    def __repr__(self):
        return repr(self.value)

    def __str__(self) -> str:
        return str(self.value)

    def get_sources(self):
        return {self.meta.source: self.meta.path()}


PRIMITIVES = (int, float, str, bytes, bool)


class Container(Node, ABC):
    meta: Meta
    _key_transform = staticmethod(identity)
    data: Union[Mapping[str, Node], Sequence[Node]]

    def __init__(self, meta) -> None:
        self.meta = meta or Meta(source=None)

    def _convert(self, key, value) -> Node:
        meta = Meta.update_path(self.meta, key)
        if value is None or isinstance(value, PRIMITIVES):
            return Value(value, meta=meta)
        elif isinstance(value, Node):
            return value
        elif isinstance(value, (list, dict)):
            container = CtxDict if isinstance(value, dict) else CtxList
            return container(value, meta=meta)
        else:
            msg = (
                "Unsupported value of type "
                f"'{type(value).__name__}' in '{meta}'"
            )
            raise TypeError(msg)

    def __repr__(self) -> str:
        return repr(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __eq__(self, o):
        container = type(self)
        if isinstance(o, container):
            return o.data == self.data
        return container(o) == self

    def __getitem__(self, key: Any):
        raise NotImplementedError

    def select(self, key: str):
        index, *rems = key.split(sep=".", maxsplit=1)
        index = index.strip()
        index = self._key_transform(index)
        try:
            d = self[index]
        except LookupError as exc:
            raise ValueError(
                f"Could not find '{index}' in {self.data}"
            ) from exc
        return d.select(rems[0]) if rems else d

    def get_sources(self):
        return {}


class CtxList(Container, MutableSequence[Node]):
    _key_transform = staticmethod(int)

    def __init__(self, values: Sequence, meta: Meta = None):
        super().__init__(meta=meta)
        self.data: List[Node] = []
        self.extend(values)

    def insert(self, index: int, value):
        self.data.insert(index, self._convert(index, value))

    def get_sources(self):
        return {self.meta.source: self.meta.path()}

    def __getitem__(self, key: Union[int, slice]):
        return self.data[key]

    def __setitem__(self, key: Union[int, slice], value) -> None:
        if isinstance(key, slice):
            self.data[value] = [
                self._convert(key, value)
                for key, value in zip(
                    range(slice.start, slice.stop, slice.step), value
                )
            ]
        else:
            self.data[key] = self._convert(key, value)

    def __delitem__(self, key: Union[int, slice]):
        del self.data[key]


class CtxDict(Container, MutableMapping):
    def __init__(self, mapping: Mapping = None, meta: Meta = None, **kwargs):
        super().__init__(meta=meta)
        self.data: Dict[str, Node] = {}
        if mapping:
            self.update(mapping)
        self.update(kwargs)

    def __setitem__(self, key: str, value):
        if not isinstance(key, str):
            # limitation for the interpolation
            # ignore other kinds of keys
            return
        self.data[key] = self._convert(key, value)

    def merge_update(self, *args: Mapping, overwrite=False):
        for d in args:
            _merge(self.data, d, overwrite=overwrite)

    def __getitem__(self, key: str):
        return self.data[key]

    def __delitem__(self, key: str):
        del self.data[key]


class Context(CtxDict):
    def __init__(self, *args, **kwargs):
        """
        Top level mutable dict, with some helpers to create context and track
        """
        super().__init__(*args, **kwargs)
        self._track = False
        self._tracked_data = defaultdict(set)

    @contextmanager
    def track(self):
        self._track = True
        yield
        self._track = False

    def _track_data(self, node: Node):
        if not self._track:
            return

        for source, keys in node.get_sources().items():
            if not source:
                continue
            params_file = self._tracked_data[source]
            keys = [keys] if isinstance(keys, str) else keys
            params_file.update(keys)

    @property
    def tracked(self):
        return self._tracked_data

    def select(self, key: str):
        node = super().select(key)
        self._track_data(node)
        return node

    @classmethod
    def load_from(cls, tree: BaseTree, file: str) -> "Context":
        _, ext = os.path.splitext(file)
        loader = LOADERS[ext]

        meta = Meta(source=file)
        return cls(loader(file, tree=tree), meta=meta)

    @classmethod
    def clone(cls, ctx: "Context") -> "Context":
        """Clones given context."""
        return cls(deepcopy(ctx.data))
