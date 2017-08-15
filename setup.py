#!/usr/bin/env python

from os.path import exists
from setuptools import setup


setup(name='dask-streams',
      version='0.0.1',
      description='Streams',
      url='http://github.com/mrocklin/streams/',
      maintainer='Matthew Rocklin',
      maintainer_email='mrocklin@gmail.com',
      license='BSD',
      keywords='streams',
      packages=['dask_streams'],
      long_description=(open('README.rst').read() if exists('README.rst')
                        else ''),
      install_requires=list(open('requirements.txt').read().strip().split('\n')),
      zip_safe=False)
