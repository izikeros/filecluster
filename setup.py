"""Setup function for filecluster package."""
from setuptools import setup, find_packages

with open("filecluster/version.py") as fp:
    exec(fp.read())


setup(
    name="filecluster",
    version=__version__,
    description="Image clustering by date",
    url="http://github.com/izikeros/filecluster",
    author="Krystian Safjan",
    author_email="ksafjan@gmail.com",
    license="MIT",
    packages=find_packages(),
)
