Contributing
====================================

Style
--------------
We attempt as much as possible to adhere to `PEP8 <https://www.python.org/dev/peps/pep-0008/>`_. Adherence to PEP8
standards can be checked using the `pycodestyle <https://pypi.python.org/pypi/pycodestyle>`_ package from your
fast-trips directory::

  pycodestyle fasttrips

Development Workflow
----------------------------
Aside from the core developer team, we use a
`fork-and-pull workflow <https://gist.github.com/Chaser324/ce0505fbed06b947d962>`_.

We use the `Git Flow <http://nvie.com/posts/a-successful-git-branching-model/>`_ branching model whereby there is always
a master and develop branch.  New features should always be developed in a branch from the develop branch.

In general, development should respond to `identified issues <https://github.com/BayAreaMetro/fast-trips/issues>`_ that
proceed along a general roadmap for Fast-Trips.  If no issue exists then please consider making one before proceeding so
that your approach can be discussed with the team.   When possible, issues should be written as
.`user stories <https://en.wikipedia.org/wiki/User_story>`_.

When possible, the team strives to adhere to
`agile software development principles <https://en.wikipedia.org/wiki/Agile_software_development#Agile_software_development_principles>`_.

Roadmap
-------

Testing
--------
There are a few dozen tests that are stored in ``\tests``.  They can be run by installing the
`PyTest <https://docs.pytest.org/en/latest/>`_ library ( ``pip install pytest`` and executing the command ``pytest``
from the command line within your ``<fast-trips-dir>``.

Most of the tests use test scenarios that can be found in the ``fasttrips/Examples`` directory.

Many (but not all) of the tests can be individually run by giving the command ``python tests/test_<TESTNAME>.py``.

Test output defaults to the folder ``fasttrips/Examples/output``

Continuous Integration
^^^^^^^^^^^^^^^^^^^^^^^

We use the **GitHub Actions** continuous integration that runs the following at each and every pull request:

  - Complete set of unit-testing on Linux, macOS and Windows
  - Documentation building test
  - Package (wheel) build test on Linux, macOS and Windows

This is a departure from the original strategy of running a small subset of tests during more intense development,
and we recommend running the entire test suite locally using the ``py.test`` command prior to issuing a pull-request.

Additionally, it is important to understand that most of the tests are system-level tests that do not guarantee correct
results so much as they make sure the system runs without an error.

For documentation-only commits, put "skip ci" somewhere in your commit message to not trigger the Travis testing.

Some regression tests have regression output that needs to be refreshed an thus have a function decorator
``@pytest.mark.skip`` so that they are skipped.

Test Descriptions
^^^^^^^^^^^^^^^^^^^

To run::

    python tests/<Test File>

+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| *Test Name*             | *Test File*                      | *Description*                                                                     |
+=========================+==================================+===================================================================================+
| Assignment Type         | ``test_assignment_type.py``      | Tests both deterministic and stochastic shortest path and hyperpaths.             |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| Simple Bunny Hop        | ``test_bunny.py``                | Tests forward and backward stochastic hyperpaths as well as a sensitivity test    |
|                         |                                  | with a different network.                                                         |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| Calculate Cost          | ``test_calculate_cost.py``       | Regression test of cost calculations.                                             |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| Convergence             | ``test_convergence.py``          |                                                                                   |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| Cost Symmetry           | ``test_cost_symmetry.py``        | Tests that the costs from the c++ pathfinding and the python calculate cost       |
|                         |                                  | functions return the same values.                                                 |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| Dispersion Levels       | ``test_dispersion.py``           | Runs dispersion levels at .0, 0.5, 0.1                                            |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| Distance Calculation    | ``test_distance.py``             |                                                                                   |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| Fares                   | ``test_fares.py``                | Tests shortcuts in fare calculation:                                              |
|                         |                                  |  - ignore pathfinding                                                             |
|                         |                                  |  - ignore pathfinding and path Enumeration                                        |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| Feedback                | ``test_feedback.py``             | Runs demand for three iterations w/ and w/out capacity constraints                |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| GTFS                    | ``test_gtfs_objects.py``         | Test that we can read and process GTFS-Plus                                       |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| Max Stop Process Count  | ``test_maxStopProcessCount.py``  | Tests 10, 50, and 100 for value of *max stop process count* – the maximum times   |
|                         |                                  | you will re-process a node (default: None)                                        |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| Overlap Functions       | ``test_overlap.py``              | Tests both overlap type and whether or not each transit segment is broken and     |
|                         |                                  | up into parts.  Tests:                                                            |
|                         |                                  | **overlap variable**: count, distance, time                                       |
|                         |                                  | **overlap split**: Boolean                                                        |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| Flexible Departure/     | ``test_pat_variation.py``        | Tests that flexible departure and arrival window penalties are working.           |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| Penalty Functions       | ``test_penalty_functions.py``    | Tests that penalty functions for flexible departure and arrival windows work.     |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| Regional Network        | ``test_psrc.py``                 | Tests that things work on a large, regional network                               |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| User Classes            | ``test_user_classes.py``         | Uses multiple user classes as defined in ``config_ft.py``                         |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+
| Function Transformations| ``test_weight_qualifiers.py``    |                                                                                   |
+-------------------------+----------------------------------+-----------------------------------------------------------------------------------+


.. note::
  Multiprocessing is not tested because it is `incompatible with PyTest <https://github.com/pytest-dev/pytest/issues/958>`_

Documentation
---------------

Functions, classes and methods should be documented using
`restructured text docstrings <https://thomas-cokelaer.info/tutorials/sphinx/docstring_python.html>`_
to be compiled in SPHINX using autodoc functionality, doxygen for c++, and breathe to link them.

An automatic CI workflow running on GitHub Actions compiles a new version of the documentation at each pull request an
stores it on GitHub for up to 30 days.

This CI workflow will fail if the documentation cannot be built. In that case, the documentation needs to be fixed
before the new branch is merged.

On merge to master, the GitHub Actions CI workflow also uploads the resulting documentation to the gh-pages branch,
keeping the online documentation in perfect synchrony with the code.

Install documentation packages
"""""""""""""""""""""""""""""""

To rebuild the documentation, you must have sphinx, numpydoc, and the read-the-docs sphinx theme installed, as well as
doxygen and the breathe to update the c++ documentation: ::

  pip install -r requirements_dev.txt

Building the C++ documentation also requires other software installation, and you can follow the directions provided on
the `doxygen page <https://www.doxygen.nl/manual/install.html>`_ to install, or if you have brew
installed on a mac, use the command: ::

  brew install doxygen

Make sure the file ``doc\doxygen.conf`` adds the breathe package location to your path using ``sys.path.append``.

Building documentation
^^^^^^^^^^^^^^^^^^^^^^^

If you have updated the c++ module, you should first update its documentation by running the following command from the
``\doc`` directory: ::

  doxygen doxygen.conf

This will output xml-formatted documentation to :code: ``doc\source\doxygen\xml``.

Then run sphinx running the following command from the :code: ``\doc`` directory: ::

  sphinx-build -b html source build

Push documentation to gh-pages branch
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In case there is a need to manually upload the compiled documentation to GitHub, it can be done via a GUI or by using
the following commands from the root fast-trips directory: ::

  tar czf /tmp/html.tgz doc/build/** ## zips and copies files to temp directory
  git checkout gh-pages              ## checks out github pages directory
  git rm -rf .                       ## clean out gh-pages branch...but not the .git folder!
  tar xzf /tmp/html.tgz              ## unzip the html that you had stashed in the temp directory
  git add .
  git commit -a -m "documentation for version x.x"
  git push origin gh-pages

.. note::
  make sure you have made any commits to the code you wanted to keep before checking out the gh-pages branch!

Todo List
-----------------

Please see the `Issues list on Github <https://github.com/BayAreaMetro/fast-trips/issues>`_ as well as the in-code todo
list below.

.. todolist::

Releases
--------
Whenever a new release is made on GitHub, all wheels for all Python versions and operating systems supported are
automatically uploaded to the `Python Package Index (PyPI) <https://pypi.python.org/pypi/fasttrips>`_, while all the
documentation is automatically updated in the github page for the project.
