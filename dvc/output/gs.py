from dvc.output.base import BaseOutput
from dvc.remote.gs import GSRemoteTree
from dvc.scm.tree import BaseTree


class GSOutput(BaseOutput):
    TREE_CLS = GSRemoteTree
