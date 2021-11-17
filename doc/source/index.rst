.. fasttrips documentation master file, created by
   sphinx-quickstart on Fri Apr 10 15:08:05 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to the Fast-Trips developers documentation!
=======================================================

This page organizes documentation that is useful for both software and model developers.  It is designed to be browsed
through from top-to-bottom starting with the objectives, then going through theory and software design, and finishing
with how to contribute.

This documentation correspond to software version:

.. git_commit_detail::
    :branch:
    :commit:
    :sha_length: 10
    :uncommitted:
    :untracked:

Software Objectives
------------------------
Objectives are driven by users and their use cases.  Generally speaking, Fast-Trips is a simulation of transit user
experience, taking into account the interactions of every transit vehicle trip and every transit passenger.  It is
capable of capturing phenomena such as bus bunching, transit overcrowding, and passenger heterogeneity.

Users
------
The team strives to keep several categories of users in mind. When possible, the functionality between these users is
separated to be the simplest possible abstraction level for that user.  For example, an analyst who is just running
fast-trips should touch the minimum number of files and parameters to run it and analyze the results.  User categories
have been generally defined as:

* **Developers**: Well-versed in the details of how Fast-Trips works, developers aim to make the user experience good
for other users
* **Modelers**: Modelers understand the algorithms and data structures that Fast-Trips uses and may edit them around the
edges to achieve their goals.
* **Analysts**: Analysts primarily interact with Fast-Trips in order to run a scenario and analyze the results.  They
should be able to easily change parameters, inputs, and run configurations and verify that Fast-Trips provided valid
results.

Use Cases
----------
Fast-trips can be used for analyzing short-term effects as well as long range planning when linked up with a
`travel demand modeling  <https://zephyrtransport.github.io/zephyr-directory/project-groups/abm-platforms/>`_ tool such
as `ActivitySim <https://github.com/UDST/activitysim>`_:

* An analyst who wants to study the effect of a schedule change on service reliability.
* An analyst who wants to evaluate a service plan for a special event.
* A modeler who wants to include capacity constraints and reliability as a performance metric for long-range planning
investments as evaluated in a long range transportation plan.

Assumptions and Prerequisites
------------------------------------

This guide assumes that you are familiar with the following:

* `Python <https://www.python.org/>`_ and the `pandas <http://pandas.pydata.org/>`_ library
* `C++ <https://en.wikipedia.org/wiki/C%2B%2B>`_ (if working in the c++ module)
* `Git version control <https://git-scm.com/book/en/v2/Getting-Started-About-Version-Control>`_
* `Jupyter notebooks <http://jupyter.org/>`_
* `Pathfinding algorithms <https://en.wikipedia.org/wiki/Pathfinding>`_
* `GTFS Transit Networks <http://gtfs.org/>`_



Contents
----------

.. toctree::
   :maxdepth: 2

   usecases
   gettingstarted
   howitworks
   io
   usage
   api
   contributing
   faq
   references


Indices and tables
===================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
