from protocol import ProtocolHandler
from errors import CommandError,Disconnect,Error

from gevent import socket
class Client(object):
    def __init__(self,host='127.0.0.1',port=31337) -> None:
        self._protocol = ProtocolHandler()
        self._socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self._socket.connect((host, port))
        self._fh = self._socket.makefile('rwb')
    
    def execute(self,*args):
        self._protocol.write_response(self._fh,args)
        resp = self._protocol.handle_request(self._fh)
        if isinstance(resp,Error):
            raise CommandError(resp.message)
        return resp
    
    def get(self, key):
        return self.execute('GET', key)

    def set(self, key, value):
        return self.execute('SET', key, value)

    def delete(self, key):
        return self.execute('DELETE', key)

    def flush(self):
        return self.execute('FLUSH')

    def mget(self, *keys):
        return self.execute('MGET', *keys)

    def mset(self, *items):
        return self.execute('MSET', *items)

if __name__ == '__main__':
    client = Client()
    print(client.mset('k1', 'v1', 'k2', ['v2-0', 1, 'v2-2'], 'k3', 'v3'))
    print(client.get('k3'))
    print(client.get('k2'))