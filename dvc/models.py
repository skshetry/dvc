from collections import defaultdict
from functools import singledispatch
from typing import Any, Dict, List, Optional, Type, TypeVar, Union

from funcy import first, lsplit
from pydantic import BaseModel as PydanticBaseModel
from pydantic import validator
from pydantic.class_validators import root_validator
from pydantic.fields import Field

from dvc.types import DictStrAny

from .stage import PipelineStage, Stage


class BaseModel(PydanticBaseModel):
    def __init__(self, **data: Any) -> None:
        """Supporting root for nested models."""
        if self.__custom_root_type__ and data.keys() != {ROOT_KEY}:
            data = {ROOT_KEY: data}
        super().__init__(**data)


class OutputFlags(BaseModel):
    """Flags for the output to decorate with"""

    cache: bool = Field(True, description="Cache output by DVC")
    persist: bool = Field(False, description="Persist output between runs")
    checkpoint: bool = Field(
        False, description="Intermediate output during an experiment"
    )
    desc: Optional[str] = Field(
        None, description="User description for this output"
    )


class PlotFlags(OutputFlags):
    """Plot attributes and output flags"""

    template: Optional[str] = Field(None, description="Default plot template")
    x: Optional[str] = Field(
        None, description="Default field name to use as x-axis data"
    )
    y: Optional[str] = Field(
        None, description="Default field name to use as y-axis data"
    )
    x_label: Optional[str] = Field(
        None, description="Default label for the x-axis"
    )
    y_label: Optional[str] = Field(
        None, description="Default label for the y-axis"
    )
    title: Optional[str] = Field(None, description="Default plot title")
    header: Optional[str] = Field(
        False, description="Whether the target CSV or TSV has a header or not"
    )


_T = TypeVar("_T")
EntryWithOptional = Union[str, Dict[str, _T]]
ListStr = List[str]
VarsType = List[EntryWithOptional[DictStrAny]]


def is_str(x):
    return isinstance(x, str)


def _merge_data(s_list):
    d = defaultdict(dict)
    for key in s_list:
        for k, flags in key.items():
            d[k].update(flags)
    return d


def _merge_params(s_list):
    d = defaultdict(list)
    for key in s_list:
        for k, params in key.items():
            d[k].extend(params)
    return d


def _chunk_dict(d):
    """Chunk a dictionary to a list of dict having single key-value pair"""
    return [{k: v} for k, v in d.items()]


class WithVars:
    vars_: VarsType = Field(
        alias="vars",
        default_factory=list,
        description="""\
        vars can be a list of either path of the file to load
        or a dictionary that could be declared locally. By default,
        the files are loaded from `params.yaml` if it exists before loading
        from the `vars` section.

        vars are merged recursively, but does not allow overwriting them.
        Locally declared vars can be a simple value or a nested dictionary.

        vars can also be loaded partially by specifying path, followed by
        colon `:`, followed by key name(s). Multiple keys can be specified by
        joining them with commas `,`. Note that partially importing a file, and
        trying to load complete file is not allowed or vice versa.
        """,
        examples=[
            ["test_params.yaml"],
            ["test_params.yaml:Train", "test_params.yaml:Train,parameters"],
            ["test_params.yaml", {"min": 5}, {"nested": {"vars": "allowed"}}],
        ],
    )


class Output(BaseModel):
    __root__: EntryWithOptional[OutputFlags]

    class Config:
        orm_mode = True


# Remember to always add the annotations here, otherwise the ordering would not work
# [ ] add a custom field to make it understand custom sorting logics?
# [x] ordering should be given from the Schema
# [x] removing defaults from
# [ ] way to `make()` stages
# [ ] way to `resolve()` variables?
# [X] `merge` while loading -> `validators` can be used for this
# [ ] way to `dump()` a stage directly
# [ ] `strict`/`loose` mode for ignoring/not ignoring keys conditionally


def from_lockfile(definition):
    definition
    pass


def from_lockfile():
    pass


def to_dvc_yaml(
    obj: PipelineStage, model: Type["StageDefinition"], suppress_em
):
    # kwargs["suppress_"] =
    definition = model.construct()
    return definition


def to_lockfile():
    pass


class StageDefinition(WithVars, BaseModel):
    cmd: Union[str, ListStr] = Field(..., description="Command to run")  # type: ignore
    wdir: Optional[str] = Field(None, description="Working directory")
    deps: ListStr = Field([], description="Dependencies for the stage")
    params: List[EntryWithOptional[ListStr]] = Field(
        [], description="Params for the stage"
    )
    frozen: bool = Field(False, description="Assume stage as unchanged")
    meta: Any = Field(None, description="Additional information/metadata")
    desc: Optional[str] = Field(
        None, description="User description for the stage"
    )
    always_changed: bool = Field(
        False, description="Assume stage as always changed"
    )
    outs: List[Output] = Field([], description="Outputs of the stage")

    metrics: List[EntryWithOptional[OutputFlags]] = Field(
        [], description="Metrics of the stage"
    )
    plots: List[EntryWithOptional[PlotFlags]] = Field(
        [], description="Plots of the stage"
    )

    # using validators to normalize
    @validator("params")
    def transform_params(cls, params) -> List[Dict[str, ListStr]]:
        defaults, others = lsplit(is_str, params)
        others.append({"params.yaml": defaults})
        return _chunk_dict(_merge_params(others))

    @validator("outs", "metrics", "plots")
    def transform_outputs(cls, outputs):
        outs_no_flag, outs_with_flags = lsplit(is_str, outputs)
        outs_with_flags.extend({out: {}} for out in outs_no_flag)
        return _chunk_dict(_merge_data(outs_with_flags))

    class Config:
        orm_mode = True


@singledispatch
def to_dict(obj):
    pass


class ForeachDo(BaseModel):
    """foreach .. do comes in pair, generates multiple stages that are similar at once"""

    # actual definition has `vars` and `foreach .. do`
    foreach: Union[list, DictStrAny, str, None] = Field(
        None,
        description="""\
        foreach data to iterate through
        
        Can be a list/dict or a str of interpolated data
        """,
    )
    do: Optional[StageDefinition] = Field(
        None, description="parametrized instance to generate stage definition"
    )

    @root_validator(pre=True)
    def check_foreach_do(cls, values):
        pairs = {"foreach", "do"}
        missing_keys = pairs - values.keys()
        if missing_keys <= pairs:
            return values
        key = first(missing_keys)
        raise ValueError(f"foreach .. do needs to be in a pair, {key} missing")


class RawDefinition(BaseModel):
    """This is the actual definition that is inside each entry for `stages`"""

    __root__: Union[ForeachDo, StageDefinition]


class DvcYAMLSchema(WithVars, BaseModel):
    stages: Dict[str, RawDefinition] = Field(
        default_factory=dict, description="List of stages"
    )

    class Config:
        title = "dvc.yaml"
