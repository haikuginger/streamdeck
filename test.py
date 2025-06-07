from streamdeck import StreamDeck

import asyncio

async def consume_all_events(sd: StreamDeck) -> None:
    async for event in sd.events():
        await asyncio.create_subprocess_exec(
            '/usr/bin/say', str(event)
        )
        print(event)

if __name__ == '__main__':
    with StreamDeck.for_purpose("testing") as sd:
        asyncio.run(consume_all_events(sd))
