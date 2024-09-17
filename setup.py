#!/usr/bin/env python

import pathlib

from setuptools import setup

VERSION = "0.0.1"

INSTALL_REQUIRES = [
    "geopandas",
    "rioxarray",
    "joblib",
    "requests",
]

EXTRAS_REQUIRE = {
    "gee": [  # Optional dependencies for gee
        "earthengine-api",
        "geedim",
    ],
    "pc": [  # Optional dependency for pc
        "planetary-computer",
        "odc-stac",
    ],
}

long_description = (pathlib.Path(__file__).parent / "README.md").read_text()

setup(
    name="terragon-downloader",
    version=VERSION,
    license="MIT",
    description="Create EO Minicubes from Polygons and simplify EO Data downloading.",
    long_description=long_description,  # Include the README as long description
    long_description_content_type="text/markdown",
    author="Adrian Höhl",
    author_email="adrian.hoehl@tum.de",
    url="https://github.com/drnhhl/terragon",
    packages=["terragon"],
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    python_requires=">=3.9",
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
