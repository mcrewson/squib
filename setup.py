#!/usr/bin/python2

from glob import glob
from distutils.core import setup

setup(
  name='squib',
  version='0.0.1',
  url='https://github.com/mcrewson/squib',
  author='Mark Crewson',
  author_email='mark@crewson.net',
  license='Apache Software License 2.0',
  description='A small metrics gathering and reporting tool.', 
  packages=['squib', 'squib.core', 'squib.oxidizers'],
  scripts=glob('bin/*'),
)
