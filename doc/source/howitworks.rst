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



Skimming
------------------
Skims are average level-of-service indicators. Given that fasttrips is trip-based and dynamic, we need to define an
averaging scheme. Since fasttrips is zone-based, we do not need to worry about spatial aggregation. We are
therefore left with the following dimensions:

1) temporal
2) personal and trip attributes:

    * value of time
    * user class
    * purpose
    * access/egress mode

Regarding 1), the user has to specify a skimming start time, a skimming end time, and a sample interval. For each
sampling interval, we build a shortest path from each origin to all destinations, calculate common skims,
and then average over time sampling points by taking the mean.

Regarding 2), the user can provide a list of combinations for which skims are sought. An example would be [(all,
work, walk, transit, walk, 10)]. The generic form is [(user_class, purpose, access_mode, transit_mode, egress_mode,
VoT)] and any number of these can be provided. Note that at the moment you will probably want to provide only one
combination of (user_class, purpose, VoT) because of the pathfinding details mentioned in :ref:`Pathfinding details`.


Running skimming
^^^^^^^^^^^^^^^^
Either post-assignment via parameter in config_ft.txt in the fasttrips section: "create_skims = True"; or on the
unassigned network (i.e. on the service schedule as per input files) via running Run.run_fasttrips_skimming().

In both cases, fasttrips needs the the options listed in the following subsection.


Skimming parameters
"""""""""""""""""""
``time_period_start``
  Start of skimming period in minutes after midnight.

``time_period_end``
  End of skimming period in minutes after midnight.

``time_period_sampling_interval``
  Sample frequency for skim path building in minutes. This means the number of skim path building runs is
  (time_period_end - time_period_start) / time_period_sampling_interval.

``find_a_good_name_here``
[(user_class, purpose, access_mode, transit_mode, demand_mode, vot)]
TODO Jan: this is not implemented yet, currently it uses configobj with indented syntax and mean vot (across the
entire provided demand)


Implemented components
^^^^^^^^^^^^^^^^^^^^^^
All times are in seconds, fares are in whatever currency unit is provided.

``invehicle_time``
  The time in seconds spent inside a PT vehicle.

``access_time``
  The time in seconds spent accessing the PT service by the specified access mode, i.e. the time to get from the origin
  to the first PT stop.

``egress_time``
  The time in seconds spent egressing from the PT service by the specified egress mode, i.e. the time to get from the
  last PT stop to the destination.

``transfer_time``
  The time in seconds spent transferring between two stops, necessarily by foot.

``wait_time``
  The time in seconds spent waiting for the next service at each transfer stop. This is zero for trips without transfer;
  there is no wait time at the first stop, see the adaption time component.

``adaption_time``
  FastTrips' deterministic pathfinder uses total travel time as its objective to minimise. It finds the next service
  that has the earliest arrival time, and then sets the departure time of the trip such that there is no wait time at
  the first stop. The time (in seconds) between the start of a given skim time sampling period and the actual
  departure time is recorded as adaption_time.

``fare``
  The fare in the provided currency unit.

``num_transfers``
  The number of transfers.

``gen_cost``
  The generalised cost calculated with the weights provided in ``pathweights_ft`` and for the specified user_class,
  purpose, access_mode, transit_mode, egress_mode, value_of_time combination as provided in ``config_ft``.


Pathfinding details
^^^^^^^^^^^^^^^^^^^
FastTrips runs skimming as a post-processing step to assignment. Skimming uses deterministic pathfinding, with the only
difference to the point-to-point implementation of the assignment being the built of a shortest path tree per origin
(i.e. one origin to all destinations). This means the deterministic pathfinder as currently implemented determines
what constitutes a shortest path and at the moment this is always with respect to time, not generalised cost.

This also means that currently, running skimming with different values of time, user classes, and purposes will not
generate different paths.


Output format and location
^^^^^^^^^^^^^^^^^^^^^^^^^^

Skims are saved to the specified output directory in a sub-directory called skims. Currently, each combination of
user class, purpose, access_mode, transit_mode, egress_mode for which skims have been requested will have its own
sub-directory, with each component a separate omx file in that directory. Each omx file contains the data and several
attributes: Skim start time (start_time), skim end time (end_time), skim sampling period (sample_interval), the name
of the skim (name), the number of zones ('num_zones'), and lastly an attribute called 'index_to_zone_ids'. This array
encodes the mapping from skim index (0-based numpy indexing) to the zone identifier used in the input data. The
position in the array corresponds to the index of the zone identifier in the skim matrix.



