import os
from functools import cached_property
from typing import Iterator

from funcy import cat

from dvc.dependency import ParamsDependency
from dvc.exceptions import OutputNotFoundError
from dvc.fs import LocalFileSystem
from dvc.fs.base import BaseFileSystem
from dvc.output import BaseOutput
from dvc.path_info import PathInfo
from dvc.repo import Repo
from dvc.utils.fs import path_isin


class Index:
    def __init__(self, repo: "Repo", fs: "BaseFileSystem") -> None:
        # not a bug, index is very much tied to the fs, not to the repo
        self.fs = fs
        self.repo: "Repo" = repo

    @property
    def outputs(self) -> Iterator["BaseOutput"]:
        for stage in self:
            yield from stage.outs

    @property
    def decorated_outputs(self) -> Iterator["BaseOutput"]:
        for output in self.outputs:
            if output.is_decorated:
                yield output

    @property
    def metrics(self) -> Iterator["BaseOutput"]:
        for output in self.outputs:
            if output.is_metric:
                yield output

    @property
    def plots(self) -> Iterator["BaseOutput"]:
        for output in self.outputs:
            if output.is_plot:
                yield output

    @property
    def dependencies(self) -> Iterator["BaseOutput"]:
        for stage in self:
            yield from stage.dependencies

    @property
    def params(self) -> Iterator["ParamsDependency"]:
        for dep in self.dependencies:
            if isinstance(dep, ParamsDependency):
                yield dep

    @cached_property
    def stages(self):
        """
        Walks down the root directory looking for Dvcfiles,
        skipping the directories that are related with
        any SCM (e.g. `.git`), DVC itself (`.dvc`), or directories
        tracked by DVC (e.g. `dvc add data` would skip `data/`)

        NOTE: For large repos, this could be an expensive
              operation. Consider using some memoization.
        """
        error_handler = self.repo.stage_collection_error_handler
        return self.repo.stage.collect_repo(onerror=error_handler)

    @cached_property
    def outs_trie(self):
        from .trie import build_outs_trie

        return build_outs_trie(self.stages)

    @cached_property
    def graph(self):
        from .graph import build_graph

        return build_graph(self.stages, self.outs_trie)

    @cached_property
    def outs_graph(self):
        from .graph import build_outs_graph

        return build_outs_graph(self.graph, self.outs_trie)

    @cached_property
    def pipelines(self):
        from .graph import get_pipelines

        return get_pipelines(self.graph)

    def used_cache(
        self,
        targets=None,
        all_branches=False,
        with_deps=False,
        all_tags=False,
        all_commits=False,
        all_experiments=False,
        remote=None,
        force=False,
        jobs=None,
        recursive=False,
        used_run_cache=None,
        revs=None,
    ):
        """Get the stages related to the given target and collect
        the `info` of its outputs.

        This is useful to know what files from the cache are _in use_
        (namely, a file described as an output on a stage).

        The scope is, by default, the working directory, but you can use
        `all_branches`/`all_tags`/`all_commits`/`all_experiments` to expand
        the scope.

        Returns:
            A dictionary with Schemes (representing output's location) mapped
            to items containing the output's `dumpd` names and the output's
            children (if the given output is a directory).
        """
        from dvc.objects.db import NamedCache

        cache = NamedCache()

        targets = targets or [None]

        pairs = cat(
            self.repo.stage.collect_granular(
                target, recursive=recursive, with_deps=with_deps
            )
            for target in targets
        )

        rev = (
            self.scm.get_rev()
            if isinstance(self.fs, LocalFileSystem)
            else self.fs.rev
        )
        suffix = f"({rev})" if rev else ""
        for stage, filter_info in pairs:
            used_cache = stage.get_used_cache(
                remote=remote, force=force, jobs=jobs, filter_info=filter_info,
            )
            cache.update(used_cache, suffix=suffix)

        return cache

    def find_outs_by_path(self, path, outs=None, recursive=False, strict=True):
        # using `outs_graph` to ensure graph checks are run
        outs = outs or self.outs_graph

        abs_path = os.path.abspath(path)
        path_info = PathInfo(abs_path)
        match = path_info.__eq__ if strict else path_info.isin_or_eq

        def func(out):
            if out.scheme == "local" and match(out.path_info):
                return True

            if recursive and out.path_info.isin(path_info):
                return True

            return False

        matched = list(filter(func, outs))
        if not matched:
            raise OutputNotFoundError(path, self)

        return matched

    def _reset(self):
        # we don't need to reset these for the indexes that are not
        # currently checked out.
        self.__dict__.pop("outs_trie", None)
        self.__dict__.pop("outs_graph", None)
        self.__dict__.pop("graph", None)
        self.__dict__.pop("stages", None)
        self.__dict__.pop("pipelines", None)

    def filter_stages(self, path):
        for stage in self:
            if path_isin(stage.path_in_repo, path):
                yield stage

    def slice(self, target_path):
        new = Index(self.repo, self.fs)
        new.stages = self.filter_stages(target_path)
        return new

    def add_artifact(self, path_info, obj, **kwargs):
        pass

    def __iter__(self):
        yield from self.stages
