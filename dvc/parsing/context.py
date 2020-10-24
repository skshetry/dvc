from dvc.parsing.interpolate import (
    _get_expr_from_match,
    _get_matches,
    _is_exact_string,
    _is_interpolated_string,
)
import os
from collections import defaultdict
from collections.abc import Mapping, MutableMapping, MutableSequence
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field, replace
from typing import Any, List, Optional, Sequence, Union

from funcy import identity

from dvc.utils.serialize import LOADERS

SeqOrMap = Union[Sequence, Mapping]


def _merge(into, update, overwrite):
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


class Container(Node):
    meta: Meta
    data: Union[list, dict]
    _key_transform = staticmethod(identity)

    def __init__(self, meta) -> None:
        self.meta = meta or Meta(source=None)

    def _convert(self, key, value):
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

    def __repr__(self):
        return repr(self.data)

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = self._convert(key, value)

    def __delitem__(self, key):
        del self.data[key]

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __eq__(self, o):
        container = type(self)
        if isinstance(o, container):
            return o.data == self.data
        return container(o) == self

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


class CtxList(Container, MutableSequence):
    _key_transform = staticmethod(int)

    def __init__(self, values: Sequence, meta: Meta = None):
        super().__init__(meta=meta)
        self.data: list = []
        self.extend(values)

    def insert(self, index: int, value):
        self.data.insert(index, self._convert(index, value))

    def get_sources(self):
        return {self.meta.source: self.meta.path()}


class CtxDict(Container, MutableMapping):
    def __init__(self, mapping: Mapping = None, meta: Meta = None, **kwargs):
        super().__init__(meta=meta)

        self.data: dict = {}
        if mapping:
            self.update(mapping)
        self.update(kwargs)

    def __setitem__(self, key, value):
        if not isinstance(key, str):
            # limitation for the interpolation
            # ignore other kinds of keys
            return
        return super().__setitem__(key, value)

    def merge_update(self, *args, overwrite=False):
        for d in args:
            _merge(self.data, d, overwrite=overwrite)


ESC_BRACE_OPEN = r"\${"
BRACE_OPEN = "${"
UNWRAP_DEFAULT = True


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

    def _track_data(self, node):
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

    def select(self, key: str, unwrap=False):
        node = super().select(key)
        self._track_data(node)
        return self._unwrap(node) if unwrap else node

    def _unwrap(self, node: Node):
        return node.value if isinstance(node, Value) else node

    @classmethod
    def load_from(cls, tree, file: str) -> "Context":
        _, ext = os.path.splitext(file)
        loader = LOADERS[ext]

        meta = Meta(source=file)
        return cls(loader(file, tree=tree), meta=meta)

    @classmethod
    def clone(cls, ctx: "Context") -> "Context":
        """Clones given context."""
        return cls(deepcopy(ctx.data))

    def _interpolate_string(self, s: str):
        matches = _get_matches(s)
        index, buf = 0, ""
        for match in matches:
            start, end = match.span(0)
            inner = _get_expr_from_match(match)
            buf += s[index:start] + str(self.select(inner))
            index = end
        return buf + s[index:]

    def format_string(self, s: str, unwrap=UNWRAP_DEFAULT):
        if not isinstance(s, str):
            return s

        matches = _get_matches(s)

        if _is_exact_string(s, matches):
            # replace "${enabled}", if `enabled` is a boolean, with it's actual
            # value rather than it's string counterparts.
            inner = _get_expr_from_match(matches[0])
            return self.select(inner, unwrap=unwrap)

        # but not "${num} days"
        s = self._interpolate_string(s)
        # regex already backtracks and avoids any `${` starting with
        # backslashes(`\`). We just need to replace those by `${`.
        return s.replace(ESC_BRACE_OPEN, BRACE_OPEN)

    def format(self, value):
        if isinstance(value, str):
            return self.format_string(value)
        if isinstance(value, Mapping):
            return type(value)(
                (self.format_string(k), self.format(v))
                for k, v in value.items()
            )
        if isinstance(value, (list, tuple, set)):
            return type(value)(self.format(k) for k in value)
        return value

    def set(self, to_set):
        for key, value in to_set.items():
            if key in self:
                raise ValueError(f"Cannot set '{key}', key already exists")
            if isinstance(value, str):
                self._check_joined_with_interpolation(key, value)
                value = self.format_string(value, unwrap=False)
            elif isinstance(value, (Sequence, Mapping)):
                self._check_nested_collection(key, value)
                self._check_interpolation_collection(key, value)
            self[key] = value

    @staticmethod
    def _check_nested_collection(key: str, value: SeqOrMap):
        values = value.values() if isinstance(value, Mapping) else value
        has_nested = any(
            not isinstance(item, str) and isinstance(item, (Mapping, Sequence))
            for item in values
        )
        if has_nested:
            raise ValueError(f"Cannot set '{key}', has nested dict/list")

    @staticmethod
    def _check_interpolation_collection(key: str, value: SeqOrMap):
        values = value.values() if isinstance(value, Mapping) else value
        interpolated = any(_is_interpolated_string(item) for item in values)
        if interpolated:
            raise ValueError(
                f"Cannot set '{key}', "
                "having interpolation inside "
                f"'{type(value).__name__}' is not supported."
            )

    @staticmethod
    def _check_joined_with_interpolation(key: str, value: str):
        matches = _get_matches(value)
        if matches and not _is_exact_string(value, matches):
            raise ValueError(
                f"Cannot set '{key}', "
                "joining string with interpolated string"
                "is not supported"
            )
