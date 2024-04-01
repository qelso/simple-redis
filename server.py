from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer

from socket import error as socket_error

from errors import CommandError, Disconnect, Error
from protocol import ProtocolHandler

import logging

class Server(object):
    def __init__(self,host='127.0.0.1',port=31337,max_clients=64):
        self._pool = Pool(max_clients)
        self._server = StreamServer(
            (host,port),
            self.connection_handler,
            spawn=self._pool)
        self._protocol = ProtocolHandler()
        self._kv = {}
        self._commands = self.get_commands()
        
    def get_commands(self):
        return {
            b'GET': self.get,
            b'SET': self.set,
            b'DELETE': self.delete,
            b'FLUSH': self.flush,
            b'MGET': self.mget,
            b'MSET': self.mset,
        }

    def connection_handler(self,conn,address):
        # Convert "conn" (a socket object) into a file-like object.
        socket_file = conn.makefile('rwb')

        # Process client requests until client disconnets.
        while True:
            try:
                data = self._protocol.handle_request(socket_file)
                print(f'Client request: {data}')
            except Disconnect:
                break

            try:
                resp = self.get_response(data)
            except CommandError as exc:
                resp = Error(exc.args[0])

            self._protocol.write_response(socket_file,resp)

    def get_response(self,data):
        # Here we'll actually unpack the data sent by the client, execute tue
        # command they specified, and pass back the return value.
        if not isinstance(data,list):
            try:
                data = data.split()
            except:
                raise CommandError('Request must be list or simple string.')
        
        if not data:
            raise CommandError('Missing command')

        command = data[0].upper()
        if command not in self._commands:
            raise CommandError('Unrecognized command: %s' % command)
        
        return self._commands[command](*data[1:])
    
    def run(self):
        self._server.serve_forever()

    def get(self, key):
        return self._kv.get(key)

    def set(self, key, value):
        self._kv[key] = value
        return 1
    
    def delete(self, key):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0
    
    def flush(self):
        kvlen = len(self._kv)
        self._kv.clear()
        return kvlen
    
    def mget(self, *keys):
        return [self._kv.get(key) for key in keys]

    def mset(self, *items):
        data = list(zip(items[::2], items[1::2]))
        for key,value in data:
            self._kv[key] = value
        return len(data)
        


if __name__ == '__main__':
    from gevent import monkey
    monkey.patch_all()
    Server().run()