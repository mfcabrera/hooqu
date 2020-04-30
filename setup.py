#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

with open('requirements.txt') as f:
    install_requires = [l.strip() for l in f]

with open('requirements_dev.txt') as f:
    tests_require = [l.strip() for l in f]

setup(
    name='hooqu',
    version='0.1.0',
    description="Data unit testing for you Python DataFrames",
    long_description=readme + '\n\n' + history,
    author="Miguel Cabrera",
    author_email='mfcabrera@gmail.com',
    url='https://github.com/mfcabrera/hooqu',
    packages=find_packages(),
    package_dir={'hooqu':
                 'hooqu'},
    include_package_data=True,
    install_requires=install_requires,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='hooqu',
    python_requires='>=3.7',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    test_suite='tests',
    extras_require={
        'testing': tests_require
    },
)
