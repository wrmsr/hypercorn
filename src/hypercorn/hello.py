import time

from .config import Config


async def hello_app(scope, recv, send):
    match scope_ty := scope['type']:
        case 'lifespan':
            while True:
                message = await recv()
                if message['type'] == 'lifespan.startup':
                    # Do some startup here!
                    await send({'type': 'lifespan.startup.complete'})

                elif message['type'] == 'lifespan.shutdown':
                    # Do some shutdown here!
                    await send({'type': 'lifespan.shutdown.complete'})
                    return

        case 'http':
            await send({
                'type': 'http.response.start',
                'status': 200,
                'headers': [
                    [b'content-type', b'text/plain'],
                ]
            })

            await send({
                'type': 'http.response.body',
                'body': f'Hello, world! The time is {time.time()}'.encode('utf-8'),
            })

        case _:
            raise ValueError(f'Unhandled scope type: {scope_ty!r}')


def _main():
    cfg = Config()

    # backend = 'asyncio'
    backend = 'trio'

    match backend:
        case 'asyncio':
            async def _asyncio_main():
                from .asyncio import serve
                await serve(hello_app, cfg)

            import asyncio
            asyncio.run(_asyncio_main())

        case 'trio':
            async def _trio_main():
                from .trio import serve
                await serve(hello_app, cfg)

            import trio
            trio.run(_trio_main)

        case _:
            raise ValueError(backend)


if __name__ == '__main__':
    _main()
