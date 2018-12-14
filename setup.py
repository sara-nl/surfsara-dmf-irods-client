#!/usr/bin/env python

from distutils.core import setup

setup(name='irods-dmf-client',
      version='0.1',
      description='Asynchronous file transfer from tape archive',
      author='Stefan Wolfsheimer',
      author_email='stefan.wolfsheimer@surfsara.nl',
      packages=['dm_irods', 'dm_irods.socket_server'],
      scripts=['dm_iconfig',
               'dm_idaemon',
               'dm_iget',
               'dm_ilist',
               'dm_iinfo',
               'dm_iput',
               'dm_icomplete'],
      install_requires=[
          "termcolor",
          "python-irodsclient"])
