import unittest
import os
import sys
import re
from tempdir import Tempdir
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from dm_irods.socket_server import ServerApp  # noqa: E402
from dm_irods.socket_server import Server  # noqa: E402
from dm_irods.socket_server import ReturnCode  # noqa: E402


class MyServer(Server):
    def __init__(self, **kwargs):
        super(MyServer, self).__init__(**kwargs)
        self.counter = 0

    def process(self, code, msg):
        self.counter += 1
        return (ReturnCode.OK, "%d %s" % (self.counter, msg))

    def process_all(self, code, msg):
        m = re.match('^lst(\\d+)$', msg)
        if m:
            n = int(m.group(1))
        else:
            n = 1
        for i in range(1, n + 1):
            yield (ReturnCode.OK, "%d %s" % (i, msg))


class TestStringMethods(unittest.TestCase):

    def test_start_stop_server(self):
        with Tempdir(prefix="Test_", remove=True) as td:
            app = ServerApp(MyServer, work_dir=td)

            status_1 = app.status()
            self.assertEqual(status_1.status, "NOT RUNNING")

            app.start()
            status_2 = app.status()
            self.assertEqual(status_2.status, "RUNNING")

            app.stop()

            status_3 = app.status()
            self.assertEqual(status_3.status, "NOT RUNNING")


if __name__ == '__main__':
    unittest.main()
