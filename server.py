import asyncio
import tempfile
import json


class OhError(Exception):
    pass


class Data:
    """Work with metrics in temp file"""

    def __init__(self):
        self.temp = tempfile.TemporaryFile()

    def put(self, key, value, timestamp):
        if self.temp.read() != b'':
            self.temp.seek(0)
            data = json.loads(self.temp.read().decode('utf-8'))
        else:
            data = dict()
        if key not in data:
            data[key] = dict()
        data[key][timestamp] = value
        self.temp.seek(0)
        data = json.dumps(data).encode('utf-8')
        self.temp.write(data)
        self.temp.seek(0)

    def get(self, key):
        self.temp.seek(0)
        data = json.loads(self.temp.read().decode('utf-8'))
        result = dict()
        if key != '*':
            data = {key: data.get(key, dict())}
        for key, timestamp in data.items():
            result[key] = sorted(timestamp.items())
        self.temp.seek(0)

        return result


class Protocol:
    """Main protocol here"""

    @staticmethod
    def encode(feedback):
        rows = []
        for respond in feedback:
            if respond is None:
                continue
            keys = respond.keys()
            for key in keys:
                values = respond[key]
                for item in values:
                    rows.append('{} {} {}'.format(key, item[1], item[0]))
        res = "ok\n"
        if rows:
            res += "\n".join(rows) + "\n"

        return res + "\n"

    @staticmethod
    def decode(data):
        feed = data.split("\n")
        result = list()
        for req in feed:
            if not req:
                continue
            try:
                info = req.strip().split()
                if info[0] == "put":
                    result.append((info[0], info[1], float(info[2]), int(info[3])))
                elif info[0] == "get":
                    result.append((info[0], info[1]))
                else:
                    raise OhError('Oh, Error :(')
            except ValueError:
                raise OhError('Oh, Error :(')

        return result


class ClientServerProtocol(asyncio.Protocol):
    """Asyncio Realization"""

    data = Data()

    def __init__(self):
        super().__init__()

        self.protocol = Protocol()
        self.runner = Runner(self.data)
        self.bytes = b''

    def process_data(self, data):
        commands = self.protocol.decode(data)
        responses = list()
        for command in commands:
            respond = self.runner.run(*command)
            responses.append(respond)

        return self.protocol.encode(responses)

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.bytes += data
        try:
            decoded = self.bytes.decode()
        except Exception:
            return
        if not decoded.endswith('\n'):
            return
        self.bytes = b''
        try:
            resp = self.process_data(decoded)
        except OhError as err:
            self.transport.write("error\n{}\n\n".format(err).encode())
            return
        self.transport.write(resp.encode())


class Runner:
    """Run method executor"""

    def __init__(self, data):
        self.data = data

    def run(self, command, *args):
        if command == "put":
            return self.data.put(*args)
        elif command == "get":
            return self.data.get(*args)
        else:
            raise OhError('Oh, Error :(')


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(ClientServerProtocol, host, port)
    server = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


run_server('127.0.0.1', 8888)




