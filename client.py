import socket
import datetime


class Client:

    def __init__(self, ip, port, timeout=None):
        self.ip = ip
        self.port = port
        try:
            self.connection = socket.create_connection((ip, port), timeout)
        except socket.error:
            raise ClientError('Can not create connection with server')

    def put(self, key, value, timestamp=None):
        if timestamp is None:
            timestamp = time.time()
        try:
            self.connection.sendall('put {} {} {}\n'.format(str(key), str(value), str(timestamp)).encode(encoding = 'UTF-8'))
        except socket.error:
            raise ClientError('Can not send data to server')
        self.feedback()

    def get(self, key):
        try:
            self.connection.sendall('get {}\n'.format(key).encode(encoding = 'UTF-8'))
        except socket.error:
            raise ClientError('Can not send data to server')
        data = self.get_feedback()
        return data

    def feedback(self):
        data = b''
        while True:
            if data[len(data) - 2:] == b'\n\n':
                break
            try:
                data += self.connection.recv(1024)
            except socket.error:
                raise ClientError('Cannot get data from server')
        data = data.decode().split('\n', 1)
        if data[0] == 'error':
            raise ClientError(data[1])
        return data[1]

    def get_feedback(self):
        feed = self.feedback().strip()
        data = dict()
        if feed == '':
            return data
        feed = feed.split('\n')
        for item in feed:
            try:
                metric = item.split()
                key = metric[0]
                value = float(metric[1])
                timestamp = int(metric[2])
                if key not in data:
                    data[key] = list()
                data[key].append((timestamp, value))
            except IndexError:
                pass
        return data


class ClientError(Exception):
    pass
