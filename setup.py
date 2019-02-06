#!/usr/bin/env python

from setuptools import setup

setup(name='irods-dmf-client',
      version='0.1',
      description='Asynchronous file transfer from tape archive',
      author='Stefan Wolfsheimer',
      author_email='stefan.wolfsheimer@surfsara.nl',
      packages=['dm_irods', 'dm_irods.socket_server'],
      entry_points={
          'console_scripts': ['dm_iconfig=dm_irods.config:dm_iconfig',
                              'dm_idaemon=dm_irods.server:dm_idaemon',
                              'dm_iget=dm_irods.get:dm_iget',
                              'dm_ilist=dm_irods.list:dm_ilist',
                              'dm_iinfo=dm_irods.info:dm_iinfo',
                              'dm_iput=dm_irods.put:dm_iput',
                              'dm_icomplete=dm_irods.complete:dm_icomplete']},
      install_requires=[
          "termcolor",
          "python-irodsclient"])
