from __future__ import annotations

from types import TracebackType
from typing import Any, Awaitable, Callable, Optional

import anyio
import anyio.abc

from ..config import Config
from ..typing import AppWrapper, ASGIReceiveCallable, ASGIReceiveEvent, ASGISendEvent, Scope


async def _handle(
    app: AppWrapper,
    config: Config,
    scope: Scope,
    receive: ASGIReceiveCallable,
    send: Callable[[Optional[ASGISendEvent]], Awaitable[None]],
    sync_spawn: Callable,
    call_soon: Callable,
) -> None:
    try:
        await app(scope, receive, send, sync_spawn, call_soon)
    except anyio.get_cancelled_exc_class():
        raise
    except BaseExceptionGroup as error:
        _, other_errors = error.split(anyio.get_cancelled_exc_class())
        if other_errors is not None:
            await config.log.exception("Error in ASGI Framework")
            await send(None)
        else:
            raise
    except Exception:
        await config.log.exception("Error in ASGI Framework")
    finally:
        await send(None)


class TaskGroup:
    def __init__(self) -> None:
        self._nursery: Optional[Any] = None
        self._nursery_manager: Optional[anyio.abc.TaskGroup] = None

    async def spawn_app(
        self,
        app: AppWrapper,
        config: Config,
        scope: Scope,
        send: Callable[[Optional[ASGISendEvent]], Awaitable[None]],
    ) -> Callable[[ASGIReceiveEvent], Awaitable[None]]:
        app_send_channel, app_receive_channel = anyio.create_memory_object_stream[bytes](config.max_app_queue_size)
        self._nursery.start_soon(
            _handle,
            app,
            config,
            scope,
            app_receive_channel.receive,
            send,
            None,  # trio.to_thread.run_sync,
            None,  # trio.from_thread.run,
        )
        return app_send_channel.send

    def spawn(self, func: Callable, *args: Any) -> None:
        self._nursery.start_soon(func, *args)

    async def __aenter__(self) -> TaskGroup:
        self._nursery_manager = anyio.create_task_group()
        self._nursery = await self._nursery_manager.__aenter__()
        return self

    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        await self._nursery_manager.__aexit__(exc_type, exc_value, tb)
        self._nursery_manager = None
        self._nursery = None
