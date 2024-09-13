#!/usr/bin/env python

from setuptools import setup

VERSION = "0.0.1"

INSTALL_REQUIRES = [
      'geopandas',
      'rioxarray',
      ]

EXTRAS_REQUIRE={
      'gee': ['earthengine-api', 'geedim'],  # Optional dependencies for gee
      'pc': ['planetary-computer', 'odc-stac', 'pystac-client'],  # Optional dependency for pc
}

setup(name='terragon',
      version=VERSION,
      license='MIT',
      description='Create EO Minicubes from Polygons and simplify EO Data downloading.',
      author='Adrian Höhl',
      author_email='adrian.hoehl@tum.de',
      url='https://github.com/drnhhl/terragon',
      packages=['terragon'],
      install_requires=INSTALL_REQUIRES,
      extras_require=EXTRAS_REQUIRE,
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
