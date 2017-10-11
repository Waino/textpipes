#!/usr/bin/env python

from ez_setup import use_setuptools
use_setuptools()

from setuptools import setup

import re
main_py = open('textpipes/__init__.py').read()
metadata = dict(re.findall("__([a-z]+)__ = '([^']+)'", main_py))

#requires = ['ftfy', 'tqdm', 'pandas']
# also requires a newer pybloom than the one in PyPI
# for optional requirements, see textpipes/cli.py

setup(name='textpipes',
      version=metadata['version'],
      author=metadata['author'],
      author_email='stig-arne.gronroos@aalto.fi',
      #url='',
      description='textpipes',
      packages=['textpipes'], #'textpipes.tests'],
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: BSD License',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          #'Topic :: Scientific/Engineering',
      ],
      license="BSD",
      scripts=['scripts/check.py',
              ],
      #install_requires=requires,
      #extras_require={
      #    'docs': [l.strip() for l in open('docs/build_requirements.txt')]
      #}
      )
