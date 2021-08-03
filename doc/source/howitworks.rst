How Fast-Trips Works
========================

Dynamic Passenger Assignment(DPA) Algorithms
------------------------------------------------
Fast-Trips is based on the work of Alireza Khani, Mark Hickman, and others while at University of Arizona.  Associated
academic papers include:

 * `Models and solution algorithms for transit and intermodal passenger assignment (development of FAST-TrIPs model) <http://arizona.openrepository.com/arizona/handle/10150/306074>`_
 * `Hyperpaths in networks based on transit schedules <http://trrjournalonline.trb.org/doi/10.3141/2284-04>`_

Some other useful resources include:

 * `Teaching Materials <https://drive.google.com/open?id=0Bz-oz0TqHWtNQVdFNXV5eGwtbms>`_ (currently in draft form) for
   the Fast-Trips algorithm.

 * `Term Glossary <https://drive.google.com/open?id=1usCw5FAjAXL44UavBKmCmdr7jFbAnQ-2meMlJwnEl5Y>`_ (currently being
   developed collaboratively)


System Design
------------------
One of the best ways to understand what is happening internally in Fast-Trips is to trace a single passenger through a
simulation.  This can be done by using the :code:`run_trace.py` script found in the :code:`/scripts` directory.


You can also review the following `code flow chart <https://docs.google.com/presentation/d/1ReNqDJP4O_2m882G3NI-4xjnsd6ORjOcDCxOQNGZN4c/edit#slide=id.p>`_ (which is a work in progress)
