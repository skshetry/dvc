from dvc.types import ListStr
from functools import wraps

from funcy import decorator
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from . import Stage


@decorator
def rwlocked(call=None, read: ListStr = None, write: ListStr = None):
    import sys
    from dvc.rwlock import rwlock
    from dvc.dependency.repo import RepoDependency

    stage: "Stage" = call._args[0]  # pylint: disable=protected-access

    assert stage.repo.lock.is_locked

    def _chain(names):
        return [
            item.path_info
            for attr in names
            for item in getattr(stage, attr)
            # There is no need to lock RepoDependency deps, as there is no
            # corresponding OutputREPO, so we can't even write it.
            if not isinstance(item, RepoDependency)
        ]

    cmd = " ".join(sys.argv)

    with rwlock(
        stage.repo.tmp_dir, cmd, _chain(read or []), _chain(write or [])
    ):
        return call()


def unlocked_repo(f):
    @wraps(f)
    def wrapper(stage: "Stage", *args, **kwargs):
        stage.repo.state.dump()
        stage.repo.lock.unlock()
        stage.repo._reset()  # pylint: disable=protected-access
        try:
            ret = f(stage, *args, **kwargs)
        finally:
            stage.repo.lock.lock()
            stage.repo.state.load()
        return ret

    return wrapper
