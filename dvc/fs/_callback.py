from functools import wraps
from typing import IO, TYPE_CHECKING, Any, Callable, Optional, cast

import fsspec

if TYPE_CHECKING:
    from typing_extensions import Self

    from dvc.progress import Tqdm


class FsspecCallback(fsspec.Callback):
    """FsspecCallback usable as a context manager, and a few helper methods."""

    def __enter__(self) -> "Self":
        return self

    def __exit__(self, *exc_args):
        return self.close()

    def close(self):
        """Handle here on exit."""

    @classmethod
    def as_tqdm_callback(
        cls, callback: Optional["FsspecCallback"] = None, **tqdm_cb_kwargs
    ):
        return callback or TqdmCallback(**tqdm_cb_kwargs)

    @classmethod
    def as_rich_callback(
        cls, callback: Optional["FsspecCallback"] = None, **rich_cb_kwargs
    ):
        return callback or RichCallback(**rich_cb_kwargs)

    @classmethod
    def as_callback(
        cls, callback: Optional["FsspecCallback"] = None
    ) -> "FsspecCallback":
        return callback or DEFAULT_CALLBACK

    def wrap_fn(self, fn: Callable):
        def wrapped(*args, **kwargs):
            res = fn(*args, **kwargs)
            self.relative_update()
            return res

        return wrapped

    def wrap_attr(self, fobj: IO, method: str = "read") -> IO:
        from tqdm.utils import CallbackIOWrapper

        wrapped = CallbackIOWrapper(self.relative_update, fobj, method)
        return cast(IO, wrapped)

    def wrap_and_branch(self, fn: Callable):
        """
        Wraps a function, and pass a new child callback to it.
        When the function completes, we increment the parent callback by 1.
        """
        from .local import localfs

        wrapped = self.wrap_fn(fn)

        def make_callback(path1, path2):
            child = self.branch(path1, localfs.path.name(path2), {})
            return self.as_callback(child)

        @wraps(fn)
        def func(path1, path2, **kwargs):
            with make_callback(path1, path2) as callback:
                return wrapped(path1, path2, callback=callback, **kwargs)

        return func


class NoOpCallback(FsspecCallback, fsspec.callbacks.NoOpCallback):
    pass


class TqdmCallback(FsspecCallback):
    def __init__(self, progress_bar: "Tqdm" = None, **pbar_kw: Any) -> None:
        from dvc.ui import ui

        self.progress_bar = progress_bar or ui.progress(**pbar_kw)
        super().__init__()

    def __enter__(self):
        self.progress_bar.__enter__()
        return self

    def close(self):
        self.progress_bar.close()

    def set_size(self, size: int = None) -> None:
        if size is not None:
            self.progress_bar.total = size
            self.progress_bar.refresh()
            super().set_size(size)

    def relative_update(self, inc: int = 1) -> None:
        self.progress_bar.update(inc)
        super().relative_update(inc)

    def absolute_update(self, value: int) -> None:
        self.progress_bar.update_to(value)
        super().absolute_update(value)

    def branch(self, path_1, path_2, kwargs):
        return TqdmCallback(bytes=True, total=-1, desc=path_2, **kwargs)


class RichCallback(FsspecCallback):
    def __init__(
        self,
        progress=None,
        desc=None,
        total=None,
        bytes=False,
        unit=None,
        disable=False,
    ) -> None:
        from dvc.ui._rich_progress import RichProgress

        self.progress = progress or RichProgress(
            transient=True, disable=disable
        )
        self._newly_created = progress is None
        self.task = self.progress.add_task(
            description=desc or "",
            total=total,
            bytes=bytes,
            visible=not disable,
            unit=f" {unit}" if unit else "",
            progress_type=None if bytes else "summary",
        )
        super().__init__()

    def __enter__(self):
        if self._newly_created:
            self.progress.__enter__()
        return self

    def close(self):
        if self._newly_created:
            self.progress.stop()
        try:
            self.progress.remove_task(self.task)
        except KeyError:
            pass

    def set_size(self, size: int = None) -> None:
        if size is not None:
            self.progress.update(self.task, total=size)
            super().set_size(size)

    def relative_update(self, inc: int = 1) -> None:
        self.progress.advance(self.task, inc)
        super().relative_update(inc)

    def absolute_update(self, value: int) -> None:
        self.progress.update(self.task, completed=value)
        super().absolute_update(value)

    def branch(self, path_1, path_2, kwargs):
        return RichCallback(
            self.progress, desc=f"  {path_2}", bytes=True, total=-1
        )


DEFAULT_CALLBACK = NoOpCallback()
