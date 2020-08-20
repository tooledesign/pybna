import os, pathlib
from setuptools import setup, find_packages
from distutils.util import convert_path

root = str(pathlib.Path(__file__).parent.absolute())
def read(fname,dir=None):
    if dir is None:
        return open(os.path.join(root, fname)).read()
    else:
        return open(os.path.join(root, dir, fname)).read()

# get data files
def package_files(directory,exts):
    paths = []
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            base, ext = os.path.splitext(filename)
            if ext in exts:
                paths.append(os.path.join('..', path, filename))
    return paths

main_ns = {}
ver_path = convert_path("pybna/_version.py")
with open(ver_path) as ver_file:
    exec(ver_file.read(), main_ns)

setup(
    name="pybna",
    version=main_ns["__version__"],
    author="Spencer Gardner",
    url="https://github.com/tooledesign/pybna",
    description="A library for measuring bike network connectivity using PeopleForBikes' Bicycle Network Analysis methodology",
    long_description=read('README.md'),
    long_description_content_type="text/markdown",
    packages=find_packages(include=["pybna","pybna.*"]),
    package_data={"": package_files(root,[".csv",".xlsx",".sql",".yaml"])},
    install_requires=[
        "pandas",
        "geopandas",
        "psycopg2-binary",
        "munch",
        "PyYAML",
        "tqdm",
        "osmnx",
        "overpass",
        "xlrd"
    ]
)
