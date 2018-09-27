import unittest
import os
import sys
import threading
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from test.tempdir import Tempdir
from socket_server import Server


class MyServer(Server):
    pass


class TestStringMethods(unittest.TestCase):
    def test_server(self):
        with Tempdir(prefix="Test_", remove=False) as td:
            socket_file = os.path.join(td, "MyServer.socket")
            server = MyServer(socket_file)
            server.start_listener()
            rthread = threading.Thread(name='run',
                                       target=server.run, args=())
            rthread.start()
            server.stop()
            rthread.join()


if __name__ == '__main__':
    unittest.main()
