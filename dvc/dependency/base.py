from dvc.output.base import (
    BaseOutput,
    OutputDoesNotExistError,
    OutputIsNotFileOrDirError,
    OutputIsStageFileError,
)


class DependencyDoesNotExistError(OutputDoesNotExistError):
    def __init__(self, path):
        msg = f"dependency '{path}' does not exist"
        super().__init__(msg)


class DependencyIsNotFileOrDirError(OutputIsNotFileOrDirError):
    def __init__(self, path):
        msg = f"dependency '{path}' is not a file or directory"
        super().__init__(msg)


class DependencyIsStageFileError(OutputIsStageFileError):
    def __init__(self, path):
        super().__init__(f"Stage file '{path}' cannot be a dependency.")


class DependencyMixin:
    IS_DEPENDENCY = True

    DoesNotExistError = DependencyDoesNotExistError
    IsNotFileOrDirError = DependencyIsNotFileOrDirError
    IsStageFileError = DependencyIsStageFileError

    def update(self, rev=None):
        pass


class BaseDependency(DependencyMixin, BaseOutput):
    pass
