from dvc.types import DictStrAny, ListStr
import logging
import os
import string
from typing import (
    Any,
    DefaultDict,
    Dict,
    Generator,
    Iterable,
    Iterator,
    Tuple,
    Type,
    Union,
    Optional,
    TYPE_CHECKING,
    List,
    TypeVar,
)
from collections import defaultdict

from funcy import cached_property, project

import dvc.dependency as dependency
import dvc.prompt as prompt
from dvc.exceptions import CheckoutError, DvcException
from dvc.utils import relpath

from . import params
from .decorators import rwlocked
from .exceptions import StageUpdateError
from .imports import sync_import, update_import
from .run import run_stage
from .utils import (
    check_circular_dependency,
    check_duplicated_arguments,
    check_missing_outputs,
    check_no_externals,
    check_stage_path,
    compute_md5,
    fill_stage_dependencies,
    fill_stage_outputs,
    get_dump,
    stage_dump_eq,
)
from dvc.dependency.local import LocalDependency
from dvc.dependency.repo import RepoDependency
from dvc.output.local import LocalOutput

if TYPE_CHECKING:
    from dvc.dvcfile import Dvcfile
    from dvc.repo import Repo
    from dvc.dependency.base import BaseDependency
    from dvc.output.base import BaseOutput
    from dvc.path_info import PathInfo
    from dvc.cache import NamedCache
    from dvc.dvcfile import PipelineFile, SingleStageFile


logger = logging.getLogger(__name__)
# Disallow all punctuation characters except hyphen and underscore
INVALID_STAGENAME_CHARS = set(string.punctuation) - {"_", "-"}


T = TypeVar("T", "Stage", "PipelineStage", covariant=True)


def loads_from(
    cls: Type[T], repo: "Repo", path: str, wdir: str, data: Dict[str, Any],
) -> T:
    kw = {
        "repo": repo,
        "path": path,
        "wdir": wdir,
        **project(
            data,
            [
                Stage.PARAM_CMD,
                Stage.PARAM_LOCKED,
                Stage.PARAM_FROZEN,
                Stage.PARAM_ALWAYS_CHANGED,
                Stage.PARAM_MD5,
                "name",
            ],
        ),
    }
    return cls(**kw)  # type: ignore


def create_stage(
    cls: Type[T], repo: "Repo", path: str, external: bool = False, **kwargs,
) -> Optional[T]:
    from dvc.dvcfile import check_dvc_filename

    wdir = os.path.abspath(kwargs.get("wdir", None) or os.curdir)
    path = os.path.abspath(path)
    check_dvc_filename(path)
    check_stage_path(repo, wdir, is_wdir=kwargs.get("wdir"))
    check_stage_path(repo, os.path.dirname(path))

    stage = loads_from(cls, repo, path, wdir, kwargs)
    fill_stage_outputs(stage, **kwargs)
    if not external:
        check_no_externals(stage)
    fill_stage_dependencies(
        stage, **project(kwargs, ["deps", "erepo", "params"])
    )
    check_circular_dependency(stage)
    check_duplicated_arguments(stage)

    if stage and stage.dvcfile.exists():
        has_persist_outs = any(out.persist for out in stage.outs)
        ignore_run_cache = (
            not kwargs.get("run_cache", True) or has_persist_outs
        )
        if has_persist_outs:
            logger.warning("Build cache is ignored when persisting outputs.")

        if not ignore_run_cache and stage.can_be_skipped:
            logger.info("Stage is cached, skipping")
            return None

    return stage


class Stage(params.StageParams):
    # pylint:disable=no-value-for-parameter
    # rwlocked() confuses pylint

    def __init__(
        self,
        repo: "Repo",
        path: str = None,
        cmd: str = None,
        wdir: str = os.curdir,
        deps: "List[BaseDependency]" = None,
        outs: "List[BaseOutput]" = None,
        md5: str = None,
        locked: bool = False,
        frozen: bool = False,
        always_changed: bool = False,
        stage_text: str = None,
        dvcfile: "Dvcfile" = None,
    ) -> None:
        if deps is None:
            deps = []
        if outs is None:
            outs = []

        self.repo = repo
        self._path = path
        self.cmd = cmd
        self.wdir = wdir
        self.outs: "List[BaseOutput]" = outs
        self.deps: "List[BaseDependency]" = deps
        self.md5 = md5
        self.frozen = locked or frozen
        self.always_changed = always_changed
        self._stage_text = stage_text
        self._dvcfile: Optional["Dvcfile"] = dvcfile

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, path):
        self._path = path
        self.__dict__.pop("path_in_repo", None)
        self.__dict__.pop("relpath", None)

    @property
    def dvcfile(self) -> "Dvcfile":
        if self.path and self._dvcfile and self.path == self._dvcfile.path:
            return self._dvcfile

        if not self.path:
            raise DvcException(
                "Stage does not have any path set "
                "and is detached from dvcfile."
            )

        from dvc.dvcfile import Dvcfile

        self._dvcfile = Dvcfile(self.repo, self.path)
        assert self._dvcfile
        return self._dvcfile

    @dvcfile.setter
    def dvcfile(self, dvcfile: "Dvcfile"):
        self._dvcfile = dvcfile

    def __repr__(self) -> str:
        return f"Stage: '{self.addressing}'"

    def __str__(self) -> str:
        return f"stage: '{self.addressing}'"

    @property
    def addressing(self) -> str:
        """
        Useful for alternative presentations where we don't need
        `Stage:` prefix.
        """
        return self.relpath if self.path else "No path"  # type: ignore

    def __hash__(self) -> int:
        return hash(self.path_in_repo)

    def __eq__(self, other) -> bool:
        return (
            self.__class__ == other.__class__
            and self.repo is other.repo
            and self.path_in_repo == other.path_in_repo
        )

    @cached_property
    def path_in_repo(self) -> str:
        return relpath(self.path, self.repo.root_dir)

    @cached_property
    def relpath(self) -> str:
        return relpath(self.path)

    @property
    def is_data_source(self) -> bool:
        """Whether the DVC-file was created with `dvc add` or `dvc import`"""
        return self.cmd is None

    @property
    def is_callback(self) -> bool:
        """
        A callback stage is always considered as changed,
        so it runs on every `dvc repro` call.
        """
        return not self.is_data_source and len(self.deps) == 0

    @property
    def is_import(self) -> bool:
        """Whether the DVC-file was created with `dvc import`."""
        return not self.cmd and len(self.deps) == 1 and len(self.outs) == 1

    @property
    def is_repo_import(self) -> bool:
        if not self.is_import:
            return False

        return isinstance(self.deps[0], dependency.RepoDependency)

    def changed_deps(self) -> bool:
        if self.frozen:
            return False

        if self.is_callback:
            logger.debug(
                '%s is a "callback" stage '
                "(has a command and no dependencies) and thus always "
                "considered as changed.",
                self,
            )
            return True

        if self.always_changed:
            return True

        return self._changed_deps()

    def _changed_deps(self) -> bool:
        for dep in self.deps:
            status = dep.status()
            if status:
                logger.debug(
                    "Dependency '{dep}' of {stage} changed because it is "
                    "'{status}'.".format(
                        dep=dep, stage=self, status=status[str(dep)]
                    )
                )
                return True
        return False

    def changed_outs(self) -> bool:
        for out in self.outs:
            status = out.status()
            if status:
                logger.debug(
                    "Output '{out}' of {stage} changed because it is "
                    "'{status}'".format(
                        out=out, stage=self, status=status[str(out)]
                    )
                )
                return True

        return False

    def changed_stage(self) -> bool:
        changed = self.md5 != self.compute_md5()
        if changed:
            logger.debug(self._changed_stage_entry())
        return changed

    @rwlocked(read=["deps", "outs"])
    def changed(self) -> bool:
        is_changed = (
            # Short-circuit order: stage md5 is fast,
            # deps are expected to change
            self.changed_stage()
            or self.changed_deps()
            or self.changed_outs()
        )
        if is_changed:
            logger.debug("%s changed.", self)
        return is_changed

    @rwlocked(write=["outs"])
    def remove_outs(
        self, ignore_remove: bool = False, force: bool = False
    ) -> None:
        """Used mainly for `dvc remove --outs` and :func:`Stage.reproduce`."""
        for out in self.outs:
            if out.persist and not force:
                out.unprotect()
                continue

            logger.debug(f"Removing output '{out}' of {self}.")
            out.remove(ignore_remove=ignore_remove)

    def unprotect_outs(self) -> None:
        for out in self.outs:
            out.unprotect()

    def ignore_remove_outs(self) -> None:
        for out in self.outs:
            out.ignore_remove()

    @rwlocked(write=["outs"])
    def remove(
        self, force: bool = False, remove_outs: bool = True, purge: bool = True
    ) -> None:
        if remove_outs:
            self.remove_outs(ignore_remove=True, force=force)
        else:
            self.unprotect_outs()
            self.ignore_remove_outs()
        if purge:
            self.dvcfile.remove_stage(self)

    @rwlocked(read=["deps"], write=["outs"])
    def reproduce(
        self, interactive: bool = False, **kwargs
    ) -> Optional["Stage"]:
        if not (kwargs.get("force", False) or self.changed()):
            logger.info("Stage '%s' didn't change, skipping", self.addressing)
            return None

        msg = (
            "Going to reproduce {stage}. "
            "Are you sure you want to continue?".format(stage=self)
        )

        if interactive and not prompt.confirm(msg):
            raise DvcException("reproduction aborted by the user")

        self.run(**kwargs)

        logger.debug(f"{self} was reproduced")

        return self

    def update(self, rev: Optional[str] = None) -> None:
        if not (self.is_repo_import or self.is_import):
            raise StageUpdateError(self.relpath)
        update_import(self, rev=rev)

    @property
    def can_be_skipped(self) -> bool:
        return (
            self.is_cached and not self.is_callback and not self.always_changed
        )

    def reload(self) -> "Stage":
        return self.dvcfile.stage

    @property
    def is_cached(self) -> bool:
        """Checks if this stage has been already ran and stored"""
        old = self.reload()
        if old.changed_outs():
            return False

        # NOTE: need to save checksums for deps in order to compare them
        # with what is written in the old stage.
        self.save_deps()
        if not stage_dump_eq(Stage, old.dumpd(), self.dumpd()):
            return False

        # NOTE: committing to prevent potential data duplication. For example
        #
        #    $ dvc config cache.type hardlink
        #    $ echo foo > foo
        #    $ dvc add foo
        #    $ rm -f foo
        #    $ echo foo > foo
        #    $ dvc add foo # should replace foo with a link to cache
        #
        old.commit()

        return True

    def dumpd(self) -> DictStrAny:
        return get_dump(self)

    def compute_md5(self) -> Optional[str]:
        # `dvc add`ed files don't need stage md5
        if self.is_data_source and not (self.is_import or self.is_repo_import):
            m = None
        else:
            m = compute_md5(self)
        logger.debug(f"Computed {self} md5: '{m}'")
        return m

    def save(self) -> None:
        self.save_deps()
        self.save_outs()
        self.md5 = self.compute_md5()

        self.repo.stage_cache.save(self)

    def save_deps(self) -> None:
        for dep in self.deps:
            dep.save()

    def save_outs(self) -> None:
        for out in self.outs:
            out.save()

    def ignore_outs(self) -> None:
        for out in self.outs:
            out.ignore()

    @staticmethod
    def _changed_entries(
        entries: Union[List[BaseDependency], List[BaseOutput]]
    ) -> ListStr:
        return [str(entry) for entry in entries if entry.changed_checksum()]

    def _changed_stage_entry(self) -> str:
        return f"'md5' of {self} changed."

    def changed_entries(self,) -> Tuple[ListStr, ListStr, Optional[str]]:
        changed_deps = self._changed_entries(self.deps)
        changed_outs = self._changed_entries(self.outs)
        return (
            changed_deps,
            changed_outs,
            self._changed_stage_entry() if self.changed_stage() else None,
        )

    @rwlocked(write=["outs"])
    def commit(self) -> None:
        for out in self.outs:
            out.commit()

    @rwlocked(read=["deps"], write=["outs"])
    def run(
        self,
        dry: bool = False,
        no_commit: bool = False,
        force: bool = False,
        run_cache: bool = True,
    ) -> None:
        if (self.cmd or self.is_import) and not self.frozen and not dry:
            self.remove_outs(ignore_remove=False, force=False)

        if not self.frozen and self.is_import:
            sync_import(self, dry, force)
        elif not self.frozen and self.cmd:
            run_stage(self, dry, force, run_cache)
        else:
            args = (
                ("outputs", "frozen ") if self.frozen else ("data sources", "")
            )
            logger.info("Verifying %s in %s%s", *args, self)
            if not dry:
                check_missing_outputs(self)

        if not dry:
            self.save()
            if not no_commit:
                self.commit()

    def _filter_outs(
        self, path_info: Optional["PathInfo"]
    ) -> Iterable["BaseOutput"]:
        def _func(o):
            return path_info.isin_or_eq(o.path_info)

        return filter(_func, self.outs) if path_info else self.outs

    @rwlocked(write=["outs"])
    def checkout(self, **kwargs):
        stats: DefaultDict[str, List["PathInfo"]] = defaultdict(list)
        for out in self._filter_outs(kwargs.get("filter_info")):
            key, outs = self._checkout(out, **kwargs)
            if key:
                stats[key].extend(outs)
        return stats

    @staticmethod
    def _checkout(
        out: BaseOutput, **kwargs
    ) -> Tuple[Optional[str], List["PathInfo"]]:
        try:
            result = out.checkout(**kwargs)
            added, modified = result or (None, None)
            if not (added or modified):
                return None, []
            return "modified" if modified else "added", [out.path_info]
        except CheckoutError as exc:
            return "failed", exc.target_infos

    @rwlocked(read=["deps", "outs"])
    def status(self, check_updates: bool = False) -> Dict[str, list]:
        ret: list = []
        show_import = self.is_repo_import and check_updates

        if not self.frozen or show_import:
            self._status_deps(ret)
        self._status_outs(ret)
        self._status_always_changed(ret)
        self._status_stage(ret)
        return {self.addressing: ret} if ret else {}

    @staticmethod
    def _status(
        entries: Union[List["BaseOutput"], List["BaseDependency"]]
    ) -> Dict[str, str]:
        ret = {}

        for entry in entries:
            ret.update(entry.status())

        return ret

    def _status_deps(self, ret: list) -> None:
        deps_status = self._status(self.deps)
        if deps_status:
            ret.append({"changed deps": deps_status})

    def _status_outs(self, ret: list) -> None:
        outs_status = self._status(self.outs)
        if outs_status:
            ret.append({"changed outs": outs_status})

    def _status_always_changed(self, ret: list) -> None:
        if self.is_callback or self.always_changed:
            ret.append("always changed")

    def _status_stage(self, ret: list) -> None:
        if self.changed_stage():
            ret.append("changed checksum")

    def already_cached(self) -> bool:
        return (
            not self.changed_stage()
            and self.deps_cached()
            and self.outs_cached()
        )

    def deps_cached(self) -> bool:
        return all(not dep.changed() for dep in self.deps)

    def outs_cached(self) -> bool:
        return all(
            not out.changed_cache() if out.use_cache else not out.changed()
            for out in self.outs
        )

    def get_all_files_number(
        self, filter_info: Optional["PathInfo"] = None
    ) -> int:
        return sum(
            out.get_files_number(filter_info)
            for out in self._filter_outs(filter_info)
        )

    def get_used_cache(self, *args, **kwargs) -> "NamedCache":
        from dvc.cache import NamedCache

        cache = NamedCache()
        for out in self._filter_outs(kwargs.get("filter_info")):
            cache.update(out.get_used_cache(*args, **kwargs))

        return cache


class PipelineStage(Stage):
    def __init__(self, *args, name=None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.name: str = name
        self.cmd_changed: bool = False

    def __eq__(self, other) -> bool:
        return super().__eq__(other) and self.name == other.name

    def __hash__(self) -> int:
        return hash((self.path_in_repo, self.name))

    @property
    def addressing(self) -> str:
        from dvc.dvcfile import PIPELINE_FILE

        if self.path and self.relpath == PIPELINE_FILE and self.name:
            return self.name
        return f"{super().addressing}:{self.name}"

    def reload(self) -> "PipelineStage":
        return self.dvcfile.stages[self.name]  # type: ignore

    @property
    def is_cached(self) -> bool:
        return self.name in self.dvcfile.stages and super().is_cached

    def _status_stage(self, ret: list) -> None:
        if self.cmd_changed:
            ret.append("changed command")

    def changed_stage(self) -> bool:
        if self.cmd_changed:
            logger.debug(self._changed_stage_entry())
        return self.cmd_changed

    def _changed_stage_entry(self) -> str:
        return f"'cmd' of {self} has changed."
