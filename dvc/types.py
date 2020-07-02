from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Set,
    Tuple,
    Union,
)


SetStr = Set[str]
ListStr = List[str]
IntStr = Union[int, str]
DictAny = Dict[Any, Any]
DictStrAny = Dict[str, Any]
AnyCallable = Callable[..., Any]
NoArgAnyCallable = Callable[[], Any]
CallableGenerator = Generator[AnyCallable, None, None]
TupleGenerator = Generator[Tuple[str, Any], None, None]
