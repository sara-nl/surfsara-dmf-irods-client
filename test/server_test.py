import unittest
import os
import sys
import threading
from .tempdir import Tempdir
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from dm_irods.socket_server.server import Server  # noqa: E402


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
