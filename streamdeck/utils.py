from itertools import chain
from typing import Awaitable, Deque, TypeVar, Tuple, Iterable
from typing_extensions import Protocol

from asyncio import gather

T = TypeVar("T")


async def gather_and_extend(
    to_extend: Deque[T], *awaitables: Awaitable[Iterable[T]]
) -> None:
    # We have to ignore the type of the following line because `gather`
    # always returns Tuple[Any, ...], and we've disallowed Any-statements.
    # We use this function to encapsulate that Any-ness and prevent it from
    # spreading anywhere else by ensuring that the only place we call
    # `gather` is within a very well-typed scope.
    results: Tuple[Iterable[T], ...] = await gather(*awaitables)  # type: ignore
    to_extend.extend(chain.from_iterable(results))
