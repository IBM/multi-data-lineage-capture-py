# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requires = f.read().splitlines()


setup(name='provlake',
      version='0.0.72',
      description='A Python lib to capture multiworkflow provenance data from Python Scripts',
      url='http://ibm.biz/provlake',
      author='IBM Research',
      license='Apache 2.0',
      install_requires=requires,
      package_dir={'': 'src'},
      packages=find_packages(where='src'),
      python_requires='>=3.6',
      include_package_data=True,
      zip_safe=False)


