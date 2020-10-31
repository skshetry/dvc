from dataclasses import dataclass, field
from typing import Optional

HASH_DIR_SUFFIX = ".dir"


@dataclass
class HashInfo:
    name: Optional[str]
    value: Optional[str]
    dir_info: Optional[dict] = field(default=None, compare=False)

    def __bool__(self):
        return bool(self.value)

    @classmethod
    def from_dict(cls, d):
        if not d:
            return cls(None, None)
        ((name, value),) = d.items()
        return cls(name, value)

    def to_dict(self):
        return {self.name: self.value} if self else {}

    @property
    def isdir(self):
        if not self:
            return False
        return self.value.endswith(HASH_DIR_SUFFIX)
