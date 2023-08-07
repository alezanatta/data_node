#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

from pathlib import Path
from glob import glob
from os.path import basename
from os.path import splitext
from setuptools import find_packages
from setuptools import setup


containing_dir = Path(__file__).parent
VERSION = (containing_dir / "VERSION").read_text().strip()

install_requires = open("src/requirements.txt").read().splitlines()
# Remove extra-index-url since it will not work when this is installed
# as a package.
install_requires = [p for p in install_requires if "extra-index-url" not in p]

setup(
    name="data_node",
    version=VERSION,
    author="Alexandre Zanatta",
    author_email="al-zanatta@hotmail.com",
    url="",
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[splitext(basename(path))[0] for path in glob("src/*.py")],
    include_package_data=True,
    zip_safe=False,
    python_requires=">=3.9",
    install_requires=install_requires,
)
