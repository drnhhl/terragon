#!/usr/bin/env python

from setuptools import setup
import pathlib

VERSION = "0.0.1"

INSTALL_REQUIRES = [
    "geopandas",
    "rioxarray",
]

EXTRAS_REQUIRE = {
    "gee": ["earthengine-api", "geedim"],  # Optional dependencies for gee
    "pc": [
        "planetary-computer",
        "odc-stac",
        "pystac-client",
    ],  # Optional dependency for pc
}

long_description = (pathlib.Path(__file__).parent / "README.md").read_text()

setup(
    name="terragon-downloader",
    version=VERSION,
    license="MIT",
    description="Create EO Minicubes from Polygons and simplify EO Data downloading.",
    long_description=long_description,  # Include the README as long description
    long_description_content_type='text/markdown',
    author="Adrian Höhl",
    author_email="adrian.hoehl@tum.de",
    url="https://github.com/drnhhl/terragon",
    packages=["terragon"],
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "Topic :: Database :: Front-Ends",
        "Topic :: Scientific/Engineering :: GIS",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ],
)
