import asyncio
import contextlib
from collections import deque
import hid
import itertools
from threading import RLock
import time
from typing import (
    Dict,
    Set,
    Deque,
    List,
    Optional,
    Iterable,
    Iterator,
    AsyncGenerator,
)
import itertools

from .events import RawEvent, EventFeed, ComplexEvent
from .utils import gather_and_extend

ELGATO_USB_VID = 0x0FD9
STREAMDECK_USB_PID = 0x0060

_STREAMDECK_PATH_BLACKLIST: Set[str] = set()
STREAMDECK_ACQUISITION_LOCK = RLock()


class StreamDeckError(Exception):
    pass


class NoStreamDecksAvailable(StreamDeckError):
    pass


@contextlib.contextmanager  # type: ignore
def acquire_streamdeck_handle(purpose: str) -> Iterator[hid.Device]:
    device_paths: Set[str] = {
        dev["path"] for dev in hid.enumerate(ELGATO_USB_VID, STREAMDECK_USB_PID)
    }
    with STREAMDECK_ACQUISITION_LOCK:
        device_paths -= _STREAMDECK_PATH_BLACKLIST
        if not device_paths:
            raise NoStreamDecksAvailable()
        else:
            devices: Dict[str, hid.Device] = {}
            for path in device_paths:
                dev = hid.Device(path=path)
                devices[path] = dev
            print(
                f"Please tap a button on the StreamDeck you want to use for {purpose}."
            )
            found_path: Optional[str] = None
            while found_path is None:
                for path, device in devices.items():
                    # Keydown...
                    if device.read(17, timeout=10):
                        found_path = path
                        _STREAMDECK_PATH_BLACKLIST.add(path)
                        # ...and keyup.
                        device.read(17)
                        break
            for found_device in devices.values():
                found_device.close()
    print(f"Picked device at {found_path}")
    final_device = hid.Device(path=found_path)
    try:
        yield final_device
    finally:
        final_device.close()
        _STREAMDECK_PATH_BLACKLIST.remove(found_path)


class ButtonEventInstance:

    button: "Button"
    event: ComplexEvent
    occurred_at: float

    def __init__(self, button: "Button", event: ComplexEvent) -> None:
        self.button = button
        self.event = event
        self.occurred_at = time.time()

    def __str__(self) -> str:
        return f"{self.button}: {self.event.name}"


class Button:

    src_idx: int
    btn_idx: int
    state: bool
    feed: EventFeed

    def __init__(self, src_idx: int, btn_idx: int) -> None:
        self.src_idx = src_idx
        self.btn_idx = btn_idx
        self.state = False
        self.feed = EventFeed()

    async def update_state(self, state_array: bytes) -> Iterable[ButtonEventInstance]:
        new_state = bool(state_array[self.src_idx])
        changed = new_state ^ self.state
        self.state = new_state
        event: Optional[RawEvent]
        if changed:
            event = RawEvent(new_state)
        else:
            event = None
        return self.produce_events(await self.feed.process_event(event))

    async def cleanup(self) -> Iterable[ButtonEventInstance]:
        return self.produce_events(await self.feed.cleanup())

    def produce_events(
        self, base_events: Iterable[ComplexEvent]
    ) -> Iterable[ButtonEventInstance]:
        for event in base_events:
            yield ButtonEventInstance(self, event)

    def __str__(self) -> str:
        return f"Button {self.btn_idx}"


class StreamDeck:

    _pending_events: Deque[ButtonEventInstance]
    device: hid.Device
    buttons: List[Button]

    def __init__(self, device: hid.Device) -> None:
        self._pending_events = deque()
        self.device = device
        self.buttons = []
        for src_idx, btn_idx in zip(
            range(1, 16),
            itertools.chain(range(5, 0, -1), range(10, 5, -1), range(15, 10, -1)),
        ):
            self.buttons.append(Button(src_idx, btn_idx))

    @classmethod
    @contextlib.contextmanager  # type: ignore
    def for_purpose(cls, purpose: str) -> Iterator["StreamDeck"]:
        with acquire_streamdeck_handle(purpose) as sd_handle:
            yield cls(sd_handle)

    async def clean(self) -> None:
        complex_event_sets = await gather_and_extend(
            self._pending_events, *(button.cleanup() for button in self.buttons)
        )

    async def process_simple_events(self) -> None:
        started = False
        state = b""
        while state or not started:
            state = self.device.read(17, timeout=10)
            if state:
                await gather_and_extend(
                    self._pending_events,
                    *(button.update_state(state) for button in self.buttons),
                )
            else:
                # Yield to the event loop in cases where no data was read
                await asyncio.sleep(0)
            started = True

    async def events(self) -> AsyncGenerator[ButtonEventInstance, None]:
        while True:
            await asyncio.gather(self.process_simple_events(), self.clean())
            while self._pending_events:
                yield self._pending_events.popleft()


class EventHandler:

    streamdeck: StreamDeck

    def __init__(self, streamdeck: StreamDeck) -> None:
        self.streamdeck = streamdeck

    async def consume_all_events(self) -> None:
        async for event in self.streamdeck.events():
            await self.consume(event)

    async def consume(self, event: ButtonEventInstance) -> None:
        print(event)
