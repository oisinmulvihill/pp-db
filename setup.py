# -*- coding: utf-8 -*-
"""
"""
from setuptools import setup, find_packages
from ConfigParser import ConfigParser
import os
def get_metadata():
    parser = ConfigParser()
    parser.read(os.path.join(os.path.dirname(__file__),'setup.cfg'))
    return dict(parser.items('metadata'))
md = get_metadata()


Name='pp-common-db'
ProjecUrl="" #""
Version=md['version']
Author='Oisin Mulvihill, Edward Easton'
AuthorEmail='oisin.mulvihill at foldingsoftware dot com, edward.easton at foldingsoftware dot com'
Maintainer='Folding Software, Flight Data Services'
Summary='This provides common database setup utilities'
License=''
ShortDescription=Summary
Description=Summary

TestSuite = ''

needed = [
    'sqlalchemy',
    'importlib',
]

EagerResources = [
    'commondb',
]

ProjectScripts = [
]

PackageData = {
    # Include every file type in the egg file:
    '': ['*.*'],
}

# Make exe versions of the scripts:
EntryPoints = {
}


setup(
#    url=ProjecUrl,
    zip_safe=False,
    name=Name,
    version=Version,
    author=Author,
    author_email=AuthorEmail,
    description=ShortDescription,
    long_description=Description,
    license=License,
    test_suite=TestSuite,
    scripts=ProjectScripts,
    install_requires=needed,
    packages=find_packages(),
    package_data=PackageData,
    eager_resources = EagerResources,
    entry_points = EntryPoints,
    namespace_packages = ['pp','pp.common'],

)
