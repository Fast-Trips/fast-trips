Getting Started
==================


Get Fast-Trips Running
------------------------

1. **Setup a Python 2.7 virtual environment and install dependencies**
This makes sure you don't interfere with other python installations. You can do this using the base virtenv package, conda, or using the Anaconda Navigator GUI.
i.e. using conda:::

  conda create -q -y -n fast-trips-env python=2.7 numpy pandas>=0.22 psutil pytest
  source activate fast-trips-env
  pip install partridge==0.6.0.dev1

Alternatively, you can install the dependencies into an existing python 2.7 environment:::

  pip install numpy pandas >=0.22 psutil pytest partridge==0.6.0.dev1

NOTE that Python 3.X will not work yet. We are working on that.

2. **Install Fast-Trips using Git or from PyPI**
Fork and clone the main fast-trips repository to a local directory <fast-trips-dir>

Switch to the branch of the repository that you want to build (usually master or develop)

| *Windows*: install Microsoft Visual C++ Compiler for Python 2.7.
| *Linux*: install the python-dev package.
| *Mac*: using standard xcode command line tools / g++ works fine.
|
To build in <fast-trips-dir>, run the following in a command prompt: ::

  python setup.py develop build_ext --inplace.

Using the develop command prompt makes sure that changes in the package are propagated to the shell without having to re-install the package.

-OR-

Install compiled Beta version of Fast-Trips stored on PyPI using the command:::

  pip install fasttrips

3. **Test Installation**

To run an example to make sure it is installed correctly, run from the `<fast-trips-dir>`:::

   python fasttrips\Examples\Bunny_Hop\run_bunny_hop.py

 (remember to use file separators appropriate for your operating system).

Go through Tutorials
------------------------

Download and complete the `Fast-Trips tutorials <https://github.com/Fast-Trips/fast-trips-tutorial>`_.
Don't forget to open and use the `associated presentation <https://docs.google.com/presentation/d/1QctTcsYDhhpqVDzXgn4Op9E8GfEYUOYyAPdHieqIFE0/edit#slide=id.p78>`_.
