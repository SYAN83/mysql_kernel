#!/bin/bash
rm -rf build/
python3 setup.py install
python3 -m mysql_kernel.install