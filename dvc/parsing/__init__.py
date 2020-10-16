import logging
import os
from copy import deepcopy
from itertools import starmap
from typing import TYPE_CHECKING

from funcy import join
from funcy.seqs import first

from dvc.dependency.param import ParamsDependency
from dvc.path_info import PathInfo
from dvc.utils.serialize import dumps_yaml

from .context import Context, CtxDict, CtxList
from .interpolate import resolve

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)

STAGES_KWD = "stages"
SET_KWD = "set"
FOREACH_KWD = "foreach"
IN_KWD = "in"
USE_KWD = "use"
VARS_KWD = "vars"
WDIR_KWD = "wdir"
DEFAULT_PARAMS_FILE = ParamsDependency.DEFAULT_PARAMS_FILE
PARAMS_KWD = "params"


class DataResolver:
    def __init__(self, repo: "Repo", yaml_wdir: PathInfo, d):
        to_import: PathInfo = yaml_wdir / d.get(USE_KWD, DEFAULT_PARAMS_FILE)
        vars_ = d.get(VARS_KWD, {})
        if os.path.exists(to_import):
            self.global_ctx = Context.load_from(
                repo.tree, str(to_import), vars_
            )
            self.global_ctx_source = to_import
        else:
            self.global_ctx_source = None
            self.global_ctx = Context.create(vars_)

        self.data = d
        self._yaml_wdir = yaml_wdir
        self.repo = repo

    def _resolve_entry(self, name: str, definition):
        context = Context.clone(self.global_ctx)
        self._set_context_from(context, definition.get(SET_KWD, {}))

        if FOREACH_KWD in definition:
            if IN_KWD not in definition:
                raise Exception("foreach needs a in")
            return self._foreach(
                context, name, definition[FOREACH_KWD], definition[IN_KWD]
            )

        return self._resolve_stage(context, name, definition)

    def resolve(self):
        stages = self.data.get(STAGES_KWD, {})
        data = join(starmap(self._resolve_entry, stages.items()))
        logger.trace("Resolved dvc.yaml: %s", dumps_yaml(data))
        return {**self.data, STAGES_KWD: data}

    def _resolve_stage(self, context: Context, name, definition):
        definition = deepcopy(definition)
        self._set_context_from(context, definition.pop(SET_KWD, {}))
        wdir = self._resolve_wdir(context, definition.get(WDIR_KWD))
        params_file = definition.get(PARAMS_KWD, [])
        contexts = []

        params_yaml_file = wdir / DEFAULT_PARAMS_FILE
        if (self.global_ctx_source != params_yaml_file) and os.path.exists(
            params_yaml_file
        ):
            contexts.append(
                Context.load_from(self.repo.tree, str(params_yaml_file))
            )
        for item in params_file:
            if item and isinstance(item, dict):
                contexts.append(
                    Context.load_from(self.repo.tree, str(wdir / first(item)))
                )

        context.merge_update(*contexts)

        stage_d = resolve(definition, context)
        params = stage_d.get(PARAMS_KWD, []) + context.tracked

        if params:
            stage_d[PARAMS_KWD] = params
        return {name: stage_d}

    def _foreach(self, context: Context, name, foreach_data, in_data):
        assert isinstance(foreach_data, str)
        iterables = resolve(foreach_data, context)

        def each_iter(value):
            c = Context.clone(context)
            if isinstance(value, tuple):
                key, val = value
            else:
                key, val = None, value
            c["item"] = val
            if key is not None:
                c["key"] = key
            suff = key or value
            assert c
            return self._resolve_stage(c, f"{name}-{suff}", in_data)

        if isinstance(iterables, (CtxList, list, tuple)):
            gen = map(each_iter, iterables)
        elif isinstance(iterables, (CtxDict, dict)):
            gen = map(each_iter, iterables.items())
        else:
            raise Exception(f"got type of {type(iterables)}")
        return join(gen)

    def _resolve_wdir(self, context, wdir):
        if not wdir:
            return self._yaml_wdir
        wdir = resolve(wdir, context)
        return self._yaml_wdir / str(wdir)

    def _set_context_from(self, context: Context, to_set):
        for key, name in to_set.items():
            value = resolve(name, context)
            context[key] = value
