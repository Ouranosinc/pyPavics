#!/usr/bin/env python
# -*- coding: utf-8 -*-


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from pavics.__meta__ import __version__, __author__, __email__

with open('README.rst') as readme_file:
    README = readme_file.read()

with open('HISTORY.rst') as history_file:
    HISTORY = history_file.read().replace('.. :changelog:', '')

REQUIREMENTS = [
    # TODO: put package requirements here
]
with open('requirements.txt','r') as requirements_file:
    REQUIREMENTS.extend([line.strip() for line in requirements_file])

TEST_REQUIREMENTS = [
    'nose',
    # TODO: put package test requirements here
]

setup(
    # -- meta information --------------------------------------------------
    name='pavics',
    version=__version__,
    description="Power Analytics and Visualization for Climate Science",
    long_description=README + '\n\n' + HISTORY,
    author=__author__,
    author_email=__email__,
    url='https://github.com/Ouranosinc/pyPavics',
    platforms=['linux_x86_64'],
    license="ISCL",
    keywords='pavics climate meteorology',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Scientific/Engineering :: Atmospheric Science',
    ],

    # -- Package structure -------------------------------------------------
    packages=[
        'pavics',
    ],
    package_dir={'pavics':
                 'pavics'},
    include_package_data=True,
    install_requires=REQUIREMENTS,
    zip_safe=False,

    # -- self - tests --------------------------------------------------------
    test_suite='tests',
    tests_require=TEST_REQUIREMENTS,

    # -- script entry points -----------------------------------------------
    entry_points={'console_scripts': []}
)
