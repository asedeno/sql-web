#!/usr/bin/env python3

import asyncio
import base64
import binascii
import json
# import logging

import websockets

# logger = logging.getLogger("websockets.server")
# logger.setLevel(logging.DEBUG)
# logger.addHandler(logging.StreamHandler())

# TODO: move to config file
ALLOWED_HOSTS = {
    'sql.mit.edu',
    'foreign-key.mit.edu',
    'multivalue-key.mit.edu',
    'primary-key.mit.edu',
    'unique-key.mit.edu',
}
REMCTL_PORT = 4373


def is_allowed_endpoint(host, port):
    return host in ALLOWED_HOSTS and port == REMCTL_PORT


class Error(Exception):
    def __init__(self, code=None, reason=None):
        super().__init__()
        if code:
            self.code = code
        if reason:
            self.reason = reason


class MessageFormatError(Error):
    code = 4000
    reason = 'Bad message format'


class ParametersError(Error):
    code = 4001
    reason = 'Bad message parameters'


class AlreadyInitializedError(Error):
    code = 4002
    reason = 'Already initialized'


class ForbiddenEndpointError(Error):
    code = 4003
    reason = 'Forbidden endpoint'


class SocketError(Error):
    code = 4004
    reason = 'Socket error'


class UninitializedError(Error):
    code = 4005
    reason = 'Uninitialized'


class MessageTypeError(Error):
    code = 4006
    reason = 'Bad message type'


class CtlfishProxy:
    class ProxyProtocol(asyncio.Protocol):
        def __init__(self, websocket):
            self.websocket = websocket

        def ws_send(self, msg):
            asyncio.create_task(self.websocket.send(json.dumps(msg)))

        def connection_made(self, transport):
            self.ws_send({
                'type': 'ready',
            })

        def data_received(self, data):
            self.ws_send({
                'type': 'data',
                'data': base64.b64encode(data).decode(),
            })

        def connection_lost(self, exc):
            args = []
            if exc:
                args = [
                    SocketError.code,
                    str(exc),
                ]
            asyncio.create_task(self.websocket.close(*args))

    def __init__(self, websocket, path):
        self.websocket = websocket
        self.path = path
        self.transport = None
        self.protocol = None

    @staticmethod
    def get_handler_name(msg):
        try:
            msg_type = msg.get('type')
        except KeyError as exc:
            raise MessageFormatError from exc
        handler_name = 'on_' + msg_type.lower().replace('-', '_').replace('.', '_')
        if not handler_name.isidentifier():
            raise MessageTypeError
        return handler_name

    async def dispatch(self, msg):
        handler_name = self.get_handler_name(msg)
        handler = getattr(self, handler_name, None)
        if handler:
            await handler(msg)
        else:
            raise MessageTypeError

    async def run(self):
        try:
            async for message in self.websocket:
                try:
                    msg = json.loads(message)
                    await self.dispatch(msg)
                except Error as err:
                    await self.websocket.close(err.code, err.reason)
                    break
        finally:
            if self.transport is not None:
                self.transport.close()

    async def on_init(self, msg):
        host = msg['host']
        port = msg['port']
        if not (isinstance(host, str) and isinstance(port, int)):
            raise ParametersError
        if self.transport is not None or self.protocol is not None:
            raise AlreadyInitializedError
        if not is_allowed_endpoint(host, port):
            raise ForbiddenEndpointError

        self.transport, self.protocol = await asyncio.get_event_loop().create_connection(
            lambda: self.ProxyProtocol(self.websocket),
            host, port)

    async def on_write(self, msg):
        if self.transport is None:
            raise UninitializedError
        data = msg.get('data', None)
        if not isinstance(data, str):
            raise ParametersError
        try:
            buf = base64.b64decode(data)
        except binascii.Error as exc:
            raise ParametersError(reason='Invalid base64') from exc
        self.transport.write(buf)

    async def on_close(self, _msg):
        if self.transport is None:
            raise UninitializedError
        await self.transport.close()
        self.transport = None
        self.protocol = None

    @classmethod
    async def server(cls, websocket, path):
        await cls(websocket, path).run()


if __name__ == '__main__':
    start_server = websockets.serve(CtlfishProxy.server, "127.0.0.1", 8090, timeout=10)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()
