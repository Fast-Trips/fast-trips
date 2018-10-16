Getting Started
==================


Get Fast-Trips Running
------------------------

1. **Setup a Python 2.7 virtual environment**
This makes sure you don't interfere with other python installations. You can do this using the base virtenv package, conda, or using the Anaconda Navigator GUI.
i.e. using conda:::

  conda create -n fasttrips python=2.7 anaconda  source activate fasttrips

NOTE that Python 3.X will not work due to dependencies on the transitfeed package.  We are `working on removing this dependency <https://github.com/BayAreaMetro/fast-trips/issues/85>`_.

2. **Add the required packages to your Virtual Environment**
Required packages include: numpy,  Pandas, and transitfeed. Many data analytics Python distributions like Anaconda bundle numpy and pandas, but they can also be installed using the command :code:`pip install <packagename>` within the virtual environment. As a last resort, Windows users can also use `these binary package installers <https://www.lfd.uci.edu/~gohlke/pythonlibs/>`_.

3. **Install Git and/or GitHub Desktop**
Fork and clone the main fast-trips repository to a local directory <fast-trips-dir>

4. **Build Fast-Trips c++ Module**
Switch to the develop branch of the repository (or whichever branch you want to build)

| *Windows*: install Microsoft Visual C++ Compiler for Python 2.7.
| *Linux*: install the python-dev package.
| *Mac*: using standard xcode command line tools / g++ works fine.
|
To build in <fast-trips-dir>, run the following in a command prompt: ::

  python setup.py develop build_ext --inplace.

Using the develop command prompt makes sure that changes in the package are propagated to the shell without having to re-install the package.

5. **Test Installation**

Run the following command from a terminal that is open in the virtual environment from the fast-trips directory: ::

  python scripts/run_example.py


Go through Tutorials
------------------------

Download and complete the `Fast-Trips tutorials <https://github.com/Fast-Trips/fast-trips-tutorial>`_.
Don't forget to open and use the `associated presentation <https://docs.google.com/presentation/d/1QctTcsYDhhpqVDzXgn4Op9E8GfEYUOYyAPdHieqIFE0/edit#slide=id.p78>`_.
