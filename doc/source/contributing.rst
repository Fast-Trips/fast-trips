Contributing
==================

Style
-------
We attempt as much as possible to adhere to `PEP8 <https://www.python.org/dev/peps/pep-0008/>`_. Adherence to PEP8 standards can be checked using the `pycodestyle <https://pypi.python.org/pypi/pycodestyle>`_ package from your fast-trips directory::

  pycodestyle fasttrips


Development Workflow
---------------------
Aside from the core developer team, we use a `fork-and-pull workflow <https://gist.github.com/Chaser324/ce0505fbed06b947d962>`_.

We use the `Git Flow <http://nvie.com/posts/a-successful-git-branching-model/>`_ branching model whereby there is always a master and develop branch.  New features should always be developed in a branch from the develop branch.

In general, development should respond to `identified issues <https://github.com/BayAreaMetro/fast-trips/issues>`_ that proceed along a general roadmap for Fast-Trips.  If no issue exists then please consider making one before proceeding so that your approach can be discussed with the team.   When possible, issues should be written as `user stories <https://en.wikipedia.org/wiki/User_story>`_.

When possible, the team strives to adhere to `agile software development principles <https://en.wikipedia.org/wiki/Agile_software_development#Agile_software_development_principles>`_.

Roadmap
-------

Testing
--------
We are still working on more comprehensive testing, but we use the `pyTest <https://docs.pytest.org/en/latest/>`_ library and put test in the :code:`/tests` folder.

Documentation
--------

Functions, classes and methods should be documented using `restructured text docstrings <https://thomas-cokelaer.info/tutorials/sphinx/docstring_python.html>`_ to be compiled in SPHINX using autodoc functionality, doxygen for c++, and breathe to link them.

The resulting HTML is then manually pushed to the gh-pages branch.

**Install documentation packages**

Do rebuild the documentation, you must have sphinx, numpydoc, and the read-the-docs sphinx theme installed: ::

  pip install sphinx numpydoc sphinx_rtd_theme breathe

To update the c++ documentation (if needed), you will also need to install doxygen and the breathe python library: ::

  pip install breathe

Follow directions on `doxygen page <https://www.stack.nl/~dimitri/doxygen/manual/install.html>`_ to install, or if you have brew installed on a mac, use the command: ::

  brew install doxygen

Make sure the file :code: `doc\doxygen.conf` adds the breathe package location to your path using :code: `sys.path.append`.

**Building documentation**

If you have updated the c++ module, you should first update its documentation by running the following command from the :code: `\doc` directory: ::

  doxygen doxygen.conf

This will output xml-formatted documentation to :code: `doc\source\doxygen\xml`.

Then run sphinx running the following command from the :code: `\doc` directory: ::

  sphinx-build -b html source build

**Push documentation to gh-pages branch**

The resulting files in the :code:`doc/html` directory should be published to the gh-pages branch in the .git repository, making sure it is clean ahead of time.  This can be done via a GUI or by using the following commands from the root fast-trips directory: ::

  tar czf /tmp/html.tgz doc/build/** ## zips and copies files to temp directory
  git checkout gh-pages              ## checks out github pages directory
  git rm -rf .                       ## clean out gh-pages branch...but not the .git folder!
  tar xzf /tmp/html.tgz              ## unzip the html that you had stashed in the temp directory
  git add .
  git commit -a -m "documentation for version x.x"
  git push origin gh-pages

NOTE: make sure you have made any commits to the code you wanted to keep before checking out the gh-pages branch!

Todo List
----------

Please see the `Issues list on Github <https://github.com/BayAreaMetro/fast-trips/issues>`_ as well as the in-code todo list below.

.. todolist::

Releases
---------
Releases are manually uploaded to the `Python Package Index (PyPI) <https://pypi.python.org/pypi/fasttrips>`_.
