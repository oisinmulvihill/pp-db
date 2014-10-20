# -*- coding: utf-8 -*-
"""
"""
from setuptools import setup, find_packages

Name = 'pp-db'
ProjectUrl = ""
Version = "1.0.0"
Author = 'Edward Easton, Oisin Mulvihill'
AuthorEmail = ''
Maintainer = ''
Summary = 'Common database utils.'
License = ''
Description = Summary
ShortDescription = Summary

needed = [
    "sqlalchemy",
    "path.py",
    "python-dateutil",
    "transaction",
    "zope.sqlalchemy",
]

test_needed = [
    "pytest",
    "pytest-cov",
    "mock",
]

test_suite = 'pp.db.tests'

EagerResources = [
    'pp',
]

ProjectScripts = [
]

PackageData = {
    '': ['*.*'],
}

EntryPoints = """
"""

setup(
    url=ProjectUrl,
    name=Name,
    zip_safe=False,
    version=Version,
    author=Author,
    author_email=AuthorEmail,
    description=ShortDescription,
    long_description=Description,
    classifiers=[
        "Topic :: Software Development :: Libraries",
    ],
    keywords='python',
    license=License,
    scripts=ProjectScripts,
    install_requires=needed,
    tests_require=test_needed,
    test_suite=test_suite,
    include_package_data=True,
    packages=find_packages(),
    package_data=PackageData,
    eager_resources=EagerResources,
    entry_points=EntryPoints,
    namespace_packages=['pp'],
)

