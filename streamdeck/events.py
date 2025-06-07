from collections import deque
from enum import auto, Enum

import time


class RawEvent(Enum):
    BUTTON_UP = False
    BUTTON_DOWN = True
    TIMEOUT = None


class ComplexEvent(Enum):
    HOLD = auto()
    RELEASE = auto()
    PRESS = auto()
    DOUBLE_PRESS = auto()


class ButtonState(Enum):
    BUTTON_UP = auto()
    BUTTON_DOWN = auto()


from typing import Deque, Callable, Optional, Tuple, Awaitable, List


class EventFeed:

    subscription_events: Deque[
        Tuple[float, Callable[[RawEvent], Awaitable[Optional[ComplexEvent]]]]
    ]

    def __init__(self) -> None:
        self.subscription_events = deque()

    async def handle_unattached_event(
        self, initial_event: RawEvent
    ) -> Optional[ComplexEvent]:
        if initial_event is RawEvent.BUTTON_UP:
            return ComplexEvent.RELEASE
        else:
            self.subscribe(self.hold_on_timeout, 250)
        return None

    async def hold_on_timeout(self, event: RawEvent) -> Optional[ComplexEvent]:
        if event is RawEvent.TIMEOUT:
            return ComplexEvent.HOLD
        elif event is RawEvent.BUTTON_UP:
            self.subscribe(self.press_on_timeout, 250)
        return None

    async def press_on_timeout(self, event: RawEvent) -> Optional[ComplexEvent]:
        if event is RawEvent.TIMEOUT:
            return ComplexEvent.PRESS
        elif event is RawEvent.BUTTON_DOWN:
            self.subscribe(self.double_press_on_release, 250)
        return None

    async def double_press_on_release(self, event: RawEvent) -> Optional[ComplexEvent]:
        if event is RawEvent.BUTTON_UP:
            return ComplexEvent.DOUBLE_PRESS
        elif event is RawEvent.TIMEOUT:
            self.subscribe(self.hold_on_timeout, 250)
            return ComplexEvent.PRESS
        return None

    def subscribe(
        self,
        subscriber: Callable[[RawEvent], Awaitable[Optional[ComplexEvent]]],
        timeout: int = 250,
    ) -> None:
        time_at_timeout = time.time() + (0.001 * timeout)
        self.subscription_events.append((time_at_timeout, subscriber))

    async def cleanup(self) -> List[ComplexEvent]:
        now = time.time()
        resulting_events = []
        if self.subscription_events:
            while self.subscription_events and self.subscription_events[0][0] < now:
                _, subscriber = self.subscription_events.popleft()
                res = await subscriber(RawEvent.TIMEOUT)
                if res is not None:
                    resulting_events.append(res)
        return resulting_events

    async def process_event(self, event: Optional[RawEvent]) -> List[ComplexEvent]:
        resulting_events = []
        resulting_events.extend(await self.cleanup())
        if event is None:
            return resulting_events
        if self.subscription_events:
            subscriptions, self.subscription_events = self.subscription_events, deque()
            for _, subscriber in subscriptions:
                res = await subscriber(event)
                if res is not None:
                    resulting_events.append(res)
        else:
            res = await self.handle_unattached_event(event)
            if res is not None:
                resulting_events.append(res)
        return resulting_events
