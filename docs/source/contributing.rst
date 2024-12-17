Development Guide and Contributing
==================================
We welcome contributions and suggestions to the project! If you found a bug, have a feature request or want to contribute to the project, please open an issue or a pull request on GitHub.

Git, GitHub and Releases
------------------------
The project uses the GitHub with the Gitflow workflow. The main branch is 'main' and the development branch is 'dev'. Feature branches are created from 'dev' and merged back into 'dev' with a pull request. If you want to contribute to the project, please fork the repository and create a pull request to the 'dev' branch.

When new releases are created they are automatically deployed to PyPi via GitHub actions. Make sure to update the version number in terragon/__init__.py before creating a new release.

Adding a new data source
------------------------
Each data provider has its own class in the package and inherits the structure from the Base class in 'terragon/base.py'. A new data provider will get a new class in the same directory. The workflow of a data provider should match the general workflow described in the demo_files.

Make sure to include tests (and later also documentation) as described in this document. 

Linting and Styling
-------------------
flake8, black, and isort are used to ensure a consitent code basis. There are also GitHub actions in place which fail when they are not met, hence use them before creating a PR. The configuration files of these tools are in the root directory.

.. code-block:: console

    # install them with:
    pip install -r requirements-dev.txt
    # execute them with:
    flake8 . && black . && isort . 

Testing
-------
The testing uses the unittest framework, each data source has its own file in the folder 'tests'. There is a base test class which implements the basic test functionality for all data providers.

.. code-block:: console

    # run tests with:
    python -m unittest discover -s ./tests -p '*.py'
    # class tests with:
    python tests/google_earth_engine.py
    # or single test with:
    python tests/google_earth_engine.py Test02GEE.test_download

Code Documentation
------------------
The documentation builds on sphinx and readthedocs. Each class/data source has its own .rst file in the folder 'docs/source/'

.. code-block:: console

    # install the dependencies for building the docs with:
    pip install -r docs/requirements-docs.txt

    # to execute it locally use:
    sphinx-build -b html docs/ docs/_build/html

Each function gets documented with a sphinx-no-type docstring and the type specifications within the function like this:

.. code-block:: python

    def download(self, img_col: ee.ImageCollection) -> Union[xr.Dataset, List]:
        """Download the clipped images from the GEE ImageCollection, store them as temporary .tif files and create a minicube.

        :param img_col: ee.ImageCollection to download
        :return: xarray.Dataset or list of filenames
        """