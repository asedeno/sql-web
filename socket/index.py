#!/usr/bin/env python3

import cgitb
cgitb.enable()

import asyncio
import datetime
import fasteners
import hashlib
import os
import pathlib
import random
import shutil
import struct
import sys
import time
import urllib.parse
import websockets

from ctlfish_proxy import CtlfishProxy

ALIVE_INTERVAL = 30  # seconds

HTACCESS_TMPL = """\
RewriteEngine On
RewriteCond %{{HTTP:Upgrade}} websocket [NC]
RewriteCond %{{HTTP:Connection}} upgrade [NC]
RewriteRule ^(.*) ws://{host}:{port}/$1 [P]
ErrorDocument 503 /socket/
"""

INDEX_TMPL = """\
<!DOCTYPE html>
<html>
<head><title>websocket proxy</title></head>
<body><p>This is a transient location for a websocket server.</p></body>
</html>
"""

################################################################################
# Prep work

with open('/etc/hostname', 'rb') as f:
    hostname = f.read()

loc = '.' + hashlib.sha1(hostname).hexdigest()
# hostname = hostname.decode().strip()
lock_file = f'{loc}.lock'
lock = fasteners.process_lock.InterProcessLock(lock_file)
htaccess = os.path.join(loc, '.htaccess')
index = os.path.join(loc, 'index.html')
if not (os.path.isdir(loc)):
    os.mkdir(loc)


################################################################################
# The websocket server

async def run_while_active(ws_server):
    lock_path = pathlib.Path(lock_file)
    while True:
        await asyncio.sleep(ALIVE_INTERVAL)
        if not ws_server.websockets:
            # If there are no active websockets, see when we were last accessed.
            # Ensure at least ALIVE_INTERVAL seconds have passed since last access.
            now = datetime.datetime.now().timestamp()
            last_touch = lock_path.stat().st_mtime
            if (now - last_touch) > ALIVE_INTERVAL:
                break


def run_ws_server():
    # Yes, I'm just being silly now.
    localhost = '127.' + '.'.join(
        str(x) for x in
        struct.unpack(
            'BBB',
            random.randint(1, 2**24-2).to_bytes(length=3, byteorder='big')))
    server = websockets.serve(CtlfishProxy.server, localhost, 0)

    asyncio.get_event_loop().run_until_complete(server)
    time.sleep(0)
    listening_socket = server.ws_server.sockets[0]
    host, port = listening_socket.getsockname()
    with open(htaccess, 'w') as f:
        f.write(HTACCESS_TMPL.format(host=host, port=port))
    with open(index, 'w') as f:
        f.write(INDEX_TMPL)

    asyncio.get_event_loop().run_until_complete(run_while_active(server.ws_server))


################################################################################
# Forking a websocket server

def daemonize():
    try:
        pid = os.fork()
        if pid > 0:
            os.wait()
            return
    except OSError as e:
        sys.exit(1)

    os.setsid()
    os.umask(0)
    os.closerange(0,1+int(max(os.listdir('/proc/self/fd'))))

    try:
        pid = os.fork()
        if pid > 0:
            # exit from second parent
            os._exit(0)
    except OSError as e:
        sys.exit(1)

    if lock.acquire(blocking=False):
        try:
            run_ws_server()
        except Exception as e:
            with open(f'{loc}.err', 'a') as f:
                f.write(str(e))
        finally:
            shutil.rmtree(loc)
            os.unlink(lock_file)
    exit(0)


################################################################################
# Redirecting the user, after maybe starting a websocket server for this server.

def main():
    if lock.acquire(blocking=False):
        # We got the lock, which means no daemon is running.
        # Clean up if needed.
        try:
            os.unlink(htaccess)
        except FileNotFoundError:
            pass
        lock.release()
        # try to spawn a websocket server for this scripts server.
        daemonize()
    else:
        # Touch the lock file to keep the server alive longer.
        pathlib.Path(lock_file).touch()
    while not os.path.exists(htaccess):
        # wait for one to be running...
        time.sleep(0.001)
    # and redirect...
    print(f'''\
Status: 302
Location: {loc}/
''')
    exit(0)


if __name__ == '__main__':
    main()
