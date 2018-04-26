#!/usr/bin/env python
from distutils.core import setup


with open('README.rst') as f:
    README = f.read()


setup(
    name='mysql_kernel',
    version='0.1',
    description='MySQL kernel for Jupyter',
    long_description=README,
    author='Shu Yan',
    author_email='yanshu.usc@gmail.com',
    url='https://github.com/SYAN83/mysql_kernel',
    packages=['mysql_kernel'],
    install_requires=[
        'jupyter_client', 'IPython', 'ipykernel'
    ],
    classifiers=[
        'Intended Audience :: End Users/Desktop',
        'Programming Language :: Python :: 3',
    ],
)
