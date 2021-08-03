
##################
Getting Started
##################

***********************
Install Fast-Trips
***********************

System Requirements:

- Fast-Trips has been tested on Windows, Ubuntu, and Mac OS
- Fast-Trips has been tested on Python 3.6, 3.7, 3.8 & 3.9

Recommended:

- Conda package manager to manage virtual environments

or

- pyenv or virtualenv to manage virtual environment using vanilla Python versions

To compile from source:

- **Windows**, `Microsoft Visual C++ Compiler for Python 3.8 <https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2019>`_
  (or for whatever Python version you have)
- **Linux**, the python-dev package

Recommended:

- GitHub Desktop
- IDE like PyCharm, Atom, Sublime Text, etc.

Fast-Trips can either be installed from source or from compiled binaries.  Compiled binaries are made from commits to
the "master" branch and are more stable, but less up-to-date than the source.

Installing stable release from PyPI
------------------------------------

Creating a virtual environment using conda (recommended)::

  conda create fast-trips-env ft-py3 python=3.8
  source activate fast-trips-env
  pip install fasttrips

Installing into an existing python installation::

  pip install fast-trips

Installing from hosted source code
--------------------------------------------------------------------
If you want a more up-to-date version or access to the develop branch,
you can install from the Github repository directly::

  pip install git+https://github.com/bayareametro/fast-trips.git@develop#egg=fasttrips

Installing from cloned source code (recommended for developers)
--------------------------------------------------------------------

1. Install [Git][git-url] and if desired, a GUI for git like [GitHub Desktop](https://desktop.github.com/)
2. [Clone](git-clone-url) or [fork-and-clone][git-fork-url] the fast-trips repository
(https://github.com/BayAreaMetro/fast-trips.git) to a local directory: `<fast-trips-dir>`. If the user plans on making
changes to the code, it is recommended that the repository be [forked][git-fork-url] before cloning.

3. Switch to the branch of the repository that you want to use by either using GitHub Desktop or from the command line::

    git checkout master

 The ``master`` branch should be the latest stable branch and the ``develop`` branch has the latest.  Features are
 developed on feature-branches.

4. To build, in the fast-trips directory ``<fast-trips-dir>``, run the following in a command prompt::

    pip install -e .

5. *Optional* Install packages for creating documentation::

    pip install -r dependencies_dev.txt


Troubleshooting:
 - *Optional* Set the ``PYTHONPATH`` environment variable to the location of your fast-trips repo, which we're calling
   ``<fast-trips-dir>``.

.. note::
 - Pandas was thoroughly tested with Pandas 1.3.0. Reverting to that version if errors occur might be necessary

Test the Install
-------------------

 - To run an example to make sure it is installed correctly, run from the `<fast-trips-dir>`::

     python fasttrips\Examples\Bunny_Hop\run_bunny_hop.py

 (remember to use file separators appropriate for your operating system).


***********************
 I/O
***********************

Fast-trips uses three sets of :ref:input files:

1. Passenger demand
2. Transit and access network
3. Configuration files

Fast-Trips summarizes the passenger assignment in three ways:

1. Path-based
2. Vehicle-based
3. Link-based

Additionally, individual passengers can be "traced" thru the system, which produces a detailed log-style accounting.

Finally, Fast-Trips produces high-level performance outputs to measure both computing and transportation system
performance.

***********************
Go through Tutorials
***********************

Download and complete the `Fast-Trips tutorials <https://github.com/Fast-Trips/fast-trips-tutorial>`_.
Don't forget to open and use the
`associated presentation <https://docs.google.com/presentation/d/1QctTcsYDhhpqVDzXgn4Op9E8GfEYUOYyAPdHieqIFE0/edit#slide=id.p78>`_.
