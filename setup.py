#!/usr/bin/env python

from setuptools import setup

setup(name='terragon',
      version='0.1.0',
      license='MIT',
      description='Create EO Minicubes from Polygons and simplify EO Data downloading.',
      author='Adrian Höhl',
      author_email='adrian.hoehl@tum.de',
      url='https://github.com/drnhhl/terragon',
      packages=['terragon'],
      install_requires=[
            'geopandas',
            'xarray',
            'rioxarray',
            'planetary_computer',
            'geedim',
            # 'xee',

            ],
      classifiers=[
      'Development Status :: 3 - Alpha',
      'Intended Audience :: Science/Research',
      'Topic :: Database :: Front-Ends',
      'Topic :: Scientific/Engineering :: GIS',
      'License :: OSI Approved :: MIT License',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: 3.4',
      'Programming Language :: Python :: 3.5',
      'Programming Language :: Python :: 3.6',
      ],
      )
