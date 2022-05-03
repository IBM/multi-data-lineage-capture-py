# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requires = f.read().splitlines()

VERSION = open('version', 'r').read().strip()

setup(name='provlake',
      version=VERSION,
      description='A Python lib to capture multiworkflow provenance data',
      long_description='ProvLake is a system that captures and integrates data processed by multiple workflows using provenance data.',
      long_description_content_type='text/x-rst',
      url='http://ibm.biz/provlake',
      author='IBM Research',
      author_email='rfsouza@br.ibm.com',
      license='Apache 2.0',
      install_requires=requires,
      package_dir={'': 'src'},
      packages=find_packages(where='src'),
      python_requires='>=3.6,<3.9',
      include_package_data=True,
      zip_safe=False)
