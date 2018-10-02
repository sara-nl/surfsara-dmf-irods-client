#!/usr/bin/env python

from distutils.core import setup

setup(name='irods-dmf-client',
      version='0.1',
      description='Asynchronous file transfer from tape archive',
      author='Stefan Wolfsheimer',
      author_email='s.wolfsheimer@surfsara.nl',
      packages=['dm_irods'],
      scripts=['dm_iconfig',
               'dm_idaemon',
               'dm_iget',
               'dm_ilist',
               'dm_iinfo',
               'dm_iput'],
      install_requires=[
          "termcolor",
          "python-irodsclient",
          "py-socket-server"])
