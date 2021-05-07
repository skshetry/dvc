from dvc.fs.base import BaseFileSystem
from dvc.objects.db.base import ObjectDB

from .index import Index


class WorkTree:
    """
    Worktree - it's not fs, but it connects the worktree with the cache/odb.

    So, it supports moving something from:

                    (using index ->)
               ─────────────────────────────>
        cache              index               worktree
               <─────────────────────────────
                 (updating index <-)
    """

    def __init__(self, odb: "ObjectDB", fs: "BaseFileSystem", index: "Index"):
        self.odb = odb
        self.fs = fs
        self.index = index

    def commit(self, target: str):
        from dvc.objects import project, save
        from dvc.objects.stage import stage

        path_info = self.fs.path_info / target
        if not self.fs.exists(path_info):
            raise Exception("does not exist")

        obj = stage(self.odb, path_info, self.fs, self.odb.fs.PARAM_CHECKSUM)
        save(self.odb, obj)
        project(self.odb, obj, self.fs, path_info, strategy="copy")

        stage = self.index.add_artifact(path_info, obj)
        return stage

    add = commit

    def checkout(self, target):
        index = self.index.slice(target)
        return self.checkout_from_index(index)

    def checkout_from_index(self, index: Index = None):
        from dvc.objects import project

        index = index or self.index
        hash_infos = {o.path_info: o.hash_info for o in index.outputs}
        for path_info, hash_info in hash_infos.items():
            project(self.odb, self.odb.get(hash_info), self.fs, path_info)

    def remove(self, target: str, remove_outs: bool = False):
        index = self.index.slice(target)
        return self.remove_from_index(index, remove_outs=remove_outs)

    def remove_from_index(
        self,
        index: Index = None,
        remove_outs: bool = False,
        purge: bool = False,
    ):
        index = index or self.index
        for stage in index:
            stage.remove(
                remove_outs=remove_outs, force=remove_outs, purge=purge
            )

        return list(index)

    def destroy(self):
        self.remove_from_index(remove_outs=False, purge=True)
        self.odb.destroy()

    def move(self):
        pass

    def status(self):
        pass
