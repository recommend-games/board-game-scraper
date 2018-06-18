#!/usr/bin/env python
# -*- coding: utf-8 -*-

''' setup '''

import io
import os

from setuptools import find_packages, setup

# Package meta-data.
NAME = 'ludoj-scraper'
DESCRIPTION = 'Ludoj scraper'
URL = 'https://bitbucket.org/MarkusShepherd/ludoj-scraper'
EMAIL = 'markus.r.shepherd@gmail.com'
AUTHOR = 'Markus Shepherd'
REQUIRES_PYTHON = '>=3.6.0'
VERSION = '0.0.1'

# What packages are required for this module to be executed?
REQUIRED = None

# The rest you shouldn't have to touch too much :)
# ------------------------------------------------
# Except, perhaps the License and Trove Classifiers!
# If you do change the License, remember to change the Trove Classifier for that!

HERE = os.path.abspath(os.path.dirname(__file__))

# Import the README and use it as the long-description.
# Note: this will only work if 'README.md' is present in your MANIFEST.in file!
with io.open(os.path.join(HERE, 'README.md'), encoding='utf-8') as f:
    LONG_DESCRIPTION = '\n' + f.read()

if REQUIRED is None:
    with io.open(os.path.join(HERE, 'requirements-top.txt'), encoding='utf-8') as f:
        REQUIRED = f.read().split()

# Where the magic happens:
setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=('tests',)),
    entry_points={'scrapy': ['settings = ludoj.settings']},
    install_requires=REQUIRED,
    include_package_data=True,
    # license='MIT',
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        # 'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy'
    ],
)
