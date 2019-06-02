from enum import auto, Enum
from time import time
from typing import Awaitable, Callable, Optional, Tuple
from urllib.parse import unquote

from .events import Body, EndBody, Event, Request, Response, StreamClosed
from ..config import Config
from ..utils import build_and_validate_headers, suppress_body, UnexpectedMessage


class ASGIHTTPState(Enum):
    # The ASGI Spec is clear that a response should not start till the
    # framework has sent at least one body message hence why this
    # state tracking is required.
    REQUEST = auto()
    RESPONSE = auto()
    CLOSED = auto()


class HTTPStream:
    def __init__(
        self,
        config: Config,
        ssl: bool,
        client: Optional[Tuple[str, int]],
        server: Optional[Tuple[str, int]],
        send: Callable[[Event], Awaitable[None]],
        spawn_app: Callable[[dict, Callable], Awaitable[Callable]],
        stream_id: int,
    ) -> None:
        self.client = client
        self.closed = False
        self.config = config
        self.response: dict
        self.scope: dict
        self.send = send
        self.scheme = "https" if ssl else "http"
        self.server = server
        self.spawn_app = spawn_app
        self.start_time: float
        self.state = ASGIHTTPState.REQUEST
        self.stream_id = stream_id

    async def handle(self, event: Event) -> None:
        if isinstance(event, Request):
            path, _, query_string = event.raw_path.partition(b"?")
            self.scope = {
                "type": "http",
                "http_version": event.http_version,
                "asgi": {"spec_version": "2.1"},
                "method": event.method,
                "scheme": self.scheme,
                "path": unquote(path.decode("ascii")),
                "query_string": query_string,
                "root_path": self.config.root_path,
                "headers": event.headers,
                "client": self.client,
                "server": self.server,
            }
            if event.http_version == "2.0":
                self.scope["extensions"] = {"http.response.push": {}}
            self.start_time = time()
            self.app_put = await self.spawn_app(self.scope, self.app_send)
        elif isinstance(event, Body):
            await self.app_put(
                {"type": "http.request", "body": bytes(event.data), "more_body": True}
            )
        elif isinstance(event, EndBody):
            await self.app_put({"type": "http.request", "body": b"", "more_body": False})
        elif isinstance(event, StreamClosed):
            await self.app_put({"type": "http.disconnect"})
            self.closed = True

    async def app_send(self, message: Optional[dict]) -> None:
        if self.closed:
            # Allow app to finish after close
            return

        if message is None:  # App has errored
            if self.state == ASGIHTTPState.REQUEST:
                await self.send(
                    Response(
                        stream_id=self.stream_id,
                        headers=[(b"content-length", b"0"), (b"connection", b"close")],
                        status_code=500,
                    )
                )
                await self.send(EndBody(stream_id=self.stream_id))
                self.state = ASGIHTTPState.CLOSED
                self.config.access_logger.access(
                    self.scope, {"status": 500, "headers": []}, time() - self.start_time
                )
            await self.send(StreamClosed(stream_id=self.stream_id))
        else:
            if message["type"] == "http.response.start" and self.state == ASGIHTTPState.REQUEST:
                self.response = message
            elif message["type"] == "http.response.push" and self.scope["http_version"] == "2.0":
                if not isinstance(message["path"], str):
                    raise TypeError(f"{message['path']} should be a str")
                headers = []
                for name, value in self.scope["headers"]:
                    if name == b":authority" or name == b":scheme":
                        headers.append((name, value))
                headers.extend(build_and_validate_headers(message["headers"]))
                await self.send(
                    Request(
                        stream_id=self.stream_id,
                        headers=headers,
                        http_version="2.0",
                        method="GET",
                        raw_path=message["path"].encode(),
                    )
                )
            elif message["type"] == "http.response.body" and self.state in {
                ASGIHTTPState.REQUEST,
                ASGIHTTPState.RESPONSE,
            }:
                if self.state == ASGIHTTPState.REQUEST:
                    headers = build_and_validate_headers(self.response.get("headers", []))
                    await self.send(
                        Response(
                            stream_id=self.stream_id,
                            headers=headers,
                            status_code=int(self.response["status"]),
                        )
                    )
                    self.state = ASGIHTTPState.RESPONSE

                if (
                    not suppress_body(self.scope["method"], int(self.response["status"]))
                    and message.get("body", b"") != b""
                ):
                    await self.send(
                        Body(stream_id=self.stream_id, data=bytes(message.get("body", b"")))
                    )

                if not message.get("more_body", False):
                    if self.state != ASGIHTTPState.CLOSED:
                        await self.send(EndBody(stream_id=self.stream_id))
                        self.state = ASGIHTTPState.CLOSED
                        self.config.access_logger.access(
                            self.scope, self.response, time() - self.start_time
                        )
            else:
                raise UnexpectedMessage(self.state, message["type"])