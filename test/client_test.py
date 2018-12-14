import unittest
import os
import sys
from tempdir import Tempdir
from server_app_test import MyServer
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from dm_irods.socket_server import ServerApp  # noqa: E402
from dm_irods.socket_server import Client  # noqa: E402
from dm_irods.socket_server import ReturnCode  # noqa: E402


REMOVE_TEMP = True
OK = ReturnCode.OK
YIELD = ReturnCode.YIELD


class TestClient(unittest.TestCase):
    def _timeout(self):
        socket_file = "/path/to/nothing.socket"
        client = Client(socket_file, conn_trials=3)
        with self.assertRaises(Exception):
            client.connect()

    def _client(self):
        with Tempdir(prefix="Test_", remove=REMOVE_TEMP) as td:
            app = ServerApp(MyServer, work_dir=td)
            status_1 = app.status()
            self.assertEqual(status_1.status, "NOT RUNNING")
            app.start()
            client = Client(app.socket_file)
            self.assertEqual(client.request('msg'), (OK, '1 msg'))
            self.assertEqual(client.request('msg'), (OK, '2 msg'))
            self.assertEqual(client.request('msg'), (OK, '3 msg'))

            status_2 = app.status()
            self.assertEqual(status_2.status, "RUNNING")

            app.stop()

            status_3 = app.status()
            self.assertEqual(status_3.status, "NOT RUNNING")

    def test_client_request_all(self):
        with Tempdir(prefix="Test_", remove=REMOVE_TEMP) as td:
            app = ServerApp(MyServer, work_dir=td)
            status_1 = app.status()
            self.assertEqual(status_1.status, "NOT RUNNING")
            app.start()

            client = Client(app.socket_file)
            data = []
            for code, msg in client.request_all('lst3'):
                data += [(code, msg)]
            self.assertEqual(data, [(OK, '1 lst3'),
                                    (OK, '2 lst3'),
                                    (OK, '3 lst3')])

            status_2 = app.status()
            self.assertEqual(status_2.status, "RUNNING")

            app.stop()

            status_3 = app.status()
            self.assertEqual(status_3.status, "NOT RUNNING")


if __name__ == '__main__':
    unittest.main()
