"""Setup function for filecluster package."""
from setuptools import find_packages
from setuptools import setup

setup(
    name="filecluster",
    version="0.1.0",
    description="Image clustering by date",
    url="http://github.com/izikeros/filecluster",
    author="Krystian Safjan",
    author_email="ksafjan@gmail.com",
    license="MIT",
    packages=find_packages(),
)
