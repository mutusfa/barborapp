#!/usr/bin/env python

from setuptools import setup

setup(
    name="barborapp",
    version="0.1",
    description="Application for Barbora Data Engineer position",
    author="Julius Juodagalvis",
    author_email="juodagalvis@protonmail.com",
    package_dir={"": "src"},
    setup_requires=["pyspark", "nose"],
)
