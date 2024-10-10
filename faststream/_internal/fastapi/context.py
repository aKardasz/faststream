import logging
from typing import Annotated, Any, Callable, Optional

from fastapi import params

from faststream._internal.constants import EMPTY
from faststream._internal.context import ContextRepo as CR
from faststream._internal.context.resolve import resolve_context_by_name


def Context(  # noqa: N802
    name: str,
    *,
    default: Any = EMPTY,
    initial: Optional[Callable[..., Any]] = None,
) -> Any:
    """Get access to objects of the Context."""
    return params.Depends(
        lambda: resolve_context_by_name(
            name=name,
            default=default,
            initial=initial,
        ),
        use_cache=True,
    )


Logger = Annotated[logging.Logger, Context("logger")]
ContextRepo = Annotated[CR, Context("context")]