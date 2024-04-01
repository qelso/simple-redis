
from errors import CommandError,Error,Disconnect
from io import BytesIO,StringIO

class ProtocolHandler(object):
    def __init__(self):
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'$': self.handle_string,
            b'*': self.handle_array,
            b'%': self.handle_dict
        }

    def handle_request(self,socket_file):
        # Parse a request from the clinet into it's component parts
        first_byte = socket_file.read(1)
        if not first_byte:
            raise Disconnect()
        
        try:
            # Delegate to the appropriate handler based on the first byte
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise CommandError('Bad request')
    
    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip(b'\r\n')
    
    def handle_error(self,socket_file):
        return Error(socket_file.readline().rstrip(b'\r\n'))
    
    def handle_integer(self,socket_file): 
        return int(socket_file.readline().rstrip(b'\r\n'))
    
    def handle_string(self,socket_file):
        # First read the length ($<length>\r\n)
        length = int(socket_file.readline().rstrip(b'\r\n'))
        if length == -1:
            return None
        length += 2
        return socket_file.read(length)[:2]
    
    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip(b'\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elements)]
    
    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip(b'\r\n'))
        elements = [self.handle_request(socket_file) for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))
 
    def write_response(self,socket_file,data):
        # Serialize the response data and send it to the client
        buf = BytesIO()
        self._write(buf,data)
        buf.seek(0)
        socket_file.write(buf.getvalue())
        socket_file.flush()
        pass

    def _write(self,buf,data):
        if isinstance(data,str):
            buf.write(f'+{data}\r\n'.encode('utf-8'))
        elif isinstance(data,bytes):
            buf.write(f'${len(data)}\r\n{data.decode()}\r\n'.encode('utf-8'))
        elif isinstance(data,int):
            buf.write(f':{data}\r\n'.encode('utf-8'))
        elif isinstance(data, Error):
            buf.write(f'-{Error.message}\r\n'.encode('utf-8'))
        elif isinstance(data, (list,tuple)):
            buf.write(f'*{len(data)}\r\n'.encode('utf-8'))
            for item in data:
                self._write(buf,item)
        elif isinstance(data, dict):
            buf.write(f'%%{len(data)}\r\n'.encode('utf-8') )
            for key in data:
                self._write(buf,key)
                self._write(buf,data[key])
        elif data is None:
            buf.write('$-1\r\n'.encode('utf-8'))
        else:
            raise CommandError('Unrecognized type: %s' % type(data)) 

