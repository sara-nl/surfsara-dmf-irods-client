import tempfile
import shutil


class Tempdir:
    def __init__(self, remove=True, **args):
        self.args = args
        self.remove = remove

    def __enter__(self):
        self.tmpdir = tempfile.mkdtemp(**self.args)
        return self.tmpdir

    def __exit__(self, type, value, traceback):
        if self.remove:
            shutil.rmtree(self.tmpdir)

    @property
    def path(self):
        return str(self.tmpdir)
