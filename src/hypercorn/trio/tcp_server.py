from __future__ import annotations

from math import inf
from typing import Any, Generator, Optional

import exceptiongroup
import trio

from .task_group import TaskGroup
from .worker_context import WorkerContext
from ..config import Config
from ..events import Closed, Event, RawData, Updated
from ..protocol import ProtocolWrapper
from ..typing import AppWrapper
from ..utils import parse_socket_addr

MAX_RECV = 2**16


class TCPServer:
    def __init__(
        self, app: AppWrapper, config: Config, context: WorkerContext, stream: trio.abc.Stream
    ) -> None:
        self.app = app
        self.config = config
        self.context = context
        self.protocol: ProtocolWrapper
        self.send_lock = trio.Lock()
        self.idle_lock = trio.Lock()
        self.stream = stream

        self._idle_handle: Optional[trio.CancelScope] = None

    def __await__(self) -> Generator[Any, None, None]:
        return self.run().__await__()

    async def run(self) -> None:
        try:
            try:
                with trio.fail_after(self.config.ssl_handshake_timeout):
                    await self.stream.do_handshake()
            except (trio.BrokenResourceError, trio.TooSlowError):
                return  # Handshake failed
            alpn_protocol = self.stream.selected_alpn_protocol()
            socket = self.stream.transport_stream.socket
            ssl = True
        except AttributeError:  # Not SSL
            alpn_protocol = "http/1.1"
            socket = self.stream.socket
            ssl = False

        def log_handler(e: Exception) -> None:
            if self.config.log.error_logger is not None:
                self.config.log.error_logger.exception("Internal hypercorn error", exc_info=e)

        try:
            with exceptiongroup.catch(
                {OSError: lambda e: None, Exception: log_handler}  # type: ignore
            ):
                client = parse_socket_addr(socket.family, socket.getpeername())
                server = parse_socket_addr(socket.family, socket.getsockname())

                async with TaskGroup() as task_group:
                    self._task_group = task_group
                    self.protocol = ProtocolWrapper(
                        self.app,
                        self.config,
                        self.context,
                        task_group,
                        ssl,
                        client,
                        server,
                        self.protocol_send,
                        alpn_protocol,
                    )
                    await self.protocol.initiate()
                    await self._start_idle()
                    await self._read_data()
        finally:
            await self._close()

    async def protocol_send(self, event: Event) -> None:
        if isinstance(event, RawData):
            async with self.send_lock:
                try:
                    with trio.CancelScope() as cancel_scope:
                        cancel_scope.shield = True
                        await self.stream.send_all(event.data)
                except (trio.BrokenResourceError, trio.ClosedResourceError):
                    await self.protocol.handle(Closed())
        elif isinstance(event, Closed):
            await self._close()
            await self.protocol.handle(Closed())
        elif isinstance(event, Updated):
            if event.idle:
                await self._start_idle()
            else:
                await self._stop_idle()

    async def _read_data(self) -> None:
        while True:
            try:
                with trio.fail_after(self.config.read_timeout or inf):
                    data = await self.stream.receive_some(MAX_RECV)
            except (
                trio.ClosedResourceError,
                trio.BrokenResourceError,
                trio.TooSlowError,
            ):
                break
            else:
                await self.protocol.handle(RawData(data))
                if data == b"":
                    break
        await self.protocol.handle(Closed())

    async def _close(self) -> None:
        try:
            await self.stream.send_eof()
        except (
            trio.BrokenResourceError,
            AttributeError,
            trio.BusyResourceError,
            trio.ClosedResourceError,
        ):
            # They're already gone, nothing to do
            # Or it is a SSL stream
            pass
        await self.stream.aclose()

    async def _initiate_server_close(self) -> None:
        await self.protocol.handle(Closed())
        await self.stream.aclose()

    async def _start_idle(self) -> None:
        async with self.idle_lock:
            if self._idle_handle is None:
                self._idle_handle = await self._task_group._nursery.start(self._run_idle)

    async def _stop_idle(self) -> None:
        async with self.idle_lock:
            if self._idle_handle is not None:
                self._idle_handle.cancel()
            self._idle_handle = None

    async def _run_idle(
        self,
        task_status: trio._core._run._TaskStatus = trio.TASK_STATUS_IGNORED,
    ) -> None:
        cancel_scope = trio.CancelScope()
        task_status.started(cancel_scope)
        with cancel_scope:
            with trio.move_on_after(self.config.keep_alive_timeout):
                await self.context.terminated.wait()

            cancel_scope.shield = True
            await self._initiate_server_close()
