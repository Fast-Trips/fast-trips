.. _dyno_demand: https://github.com/osplanning-data-standards/dyno-demand
.. _gtfs_plus:   https://github.com/osplanning-data-standards/GTFS-PLUS
.. _dyno_path:   https://github.com/osplanning-data-standards/dyno-path
.. _network_wrangler: https://github.com/sfcta/networkwrangler
.. _gtfs_ride: https://github.com/ODOT-PTS/GTFS-ride

.. _io:
Input/Output Files
====================



.. _input_files:

The input to fast-trips consists of:
 - A Transit Network directory, including schedules, access, egress and transfer information, specified by the `gtfs_plus`_
 - A Transit Network directory, including schedules, access, egress and transfer information, specified by the `gtfs_plus`_
 - A Transit Demand directory, including persons, households and trips, specified by the `dyno_demand`_
 - fast-trips Configuration, specified below

.. _passenger_demand:

Passenger Demand
------------------

Passenger demand is specified in the the `dyno_demand`_ format of one to three csv files.
 * ``trip_list.txt``
 * ``person.txt``
 * ``household.txt``

``trip_list.txt``

 * File MUST contain a record for each trip to be assigned.
 * File MUST be a valid CSV file.
 * The first line of each file MUST contain case-sensitive field names.
 * Field names MUST NOT contain tabs, carriage returns or new lines.

+---------------------+------------+----------------------------------------------+
| Field               | DType      | Description                                  |
+=====================+============+==============================================+
| ``person_id``       |int or str  | ID that uniquely identifies the traveller.   |
|                     |            | Use 0 (zero) to identify trips that do not   |
|                     |            | have a disaggregate person-record            |
|                     |            | associated with them.                        |
+---------------------+------------+----------------------------------------------+
| ``person_trip_id``  |int or str  | ID that uniquely identifies the trip within  |
|                     |            | a given household/person.                    |
|                     |            | ID MAY be sequential.                        |
+---------------------+------------+----------------------------------------------+
| ``o_taz``           | int or str | Trip origin zone                             |
+---------------------+------------+----------------------------------------------+
| ``d_taz``           | int or str | Trip destination zone                        |
+---------------------+------------+----------------------------------------------+
| ``mode``            | str        | Trip mode, which must match a valid          |
|                     |            | specification for route choice               |
|                     |            | and the modal hierarchy.                     |
|                     |            | For transit, the mode should encapsulate     |
|                     |            | access + egress modes separated by hyphens.  |
|                     |            | Example:                                     |
|                     |            |  - ``walk-local_bus-walk``                   |
|                     |            |  - ``PNR-commuter_rail-walk``                |
|                     |            | Example main modes:                          |
|                     |            |  - ``local_bus``                             |
|                     |            |  - ``premium_bus``                           |
|                     |            |  - ``light_rail``                            |
|                     |            |  - ``commuter_rail``                         |
|                     |            |  - ``street_car``                            |
|                     |            |  - ``ferry``                                 |
|                     |            | Example access or egress modes:              |
|                     |            |  - ``walk``                                  |
|                     |            |  - ``bike_own``                              |
|                     |            |  - ``bike_share``                            |
|                     |            |  - ``PNR``                                   |
|                     |            |  - ``KNR``                                   |
+---------------------+------------+----------------------------------------------+
| ``purpose``         | str        | Trip purpose, which can include any          |
|                     |            | segmentation of purpose that is deemed       |
|                     |            | appropriate for segmenting the route choice  |
|                     |            | model so long as there are corresponding     |
|                     |            | parameters specified in the route choice     |
|                     |            | controls file.                               |
|                     |            | Examples include:                            |
|                     |            |  - ``work``                                  |
|                     |            |  - ``school``                                |
|                     |            |  - ``personal_business``                     |
|                     |            |  - ``shopping``                              |
|                     |            |  - ``meal``                                  |
|                     |            |  - ``social``                                |
|                     |            |  - ``work_based``                            |
|                     |            |  - ``other``                                 |
|                     |            |  - ``visitor``                               |
+---------------------+------------+----------------------------------------------+
| ``departure_time``  | HH:MM:SS   | Desired departure time.                      |
+---------------------+------------+----------------------------------------------+
| ``arrival_time``    | HH:MM:SS   | Desired arrival time.                        |
+---------------------+------------+----------------------------------------------+
| ``time_target``     | str        | Arrival/Departure rigidity indicator.        |
|                     |            | Options include:                             |
|                     |            |   ``arrival``: arrival time is more important|
|                     |            |   ``departure``: dept time is more important |
+---------------------+------------+----------------------------------------------+
| ``vot``             | float      | Value of time for trip in dollars / hour     |
+---------------------+------------+----------------------------------------------+
| ``pnr_ids``         | list of int| Available park and rides.  A comma-delimited |
|                     |            | list of stations within brackets.            |
|                     |            | Example: ``[1219, 3354, 9485]``              |
|                     |            | An empty list implies any accessible park    |
|                     |            | and ride can be used.                        |
+---------------------+------------+----------------------------------------------+
| ``person_tour_id``  | int or str | ID that uniquely identifies the tour within  |
|                     |            | a given household/person.                    |
|                     |            | ID MAY be sequential.                        |
+---------------------+------------+----------------------------------------------+


.. _transport_network:

Transport Network
-------------------

The transport network is specified in the the `gtfs_plus`_ format of text files.
It can be generated by augmenting existing GTFS files, or by converting travel model files to `gtfs_plus`_ using a tool
like `network_wrangler`_

Fares
^^^^^^
`GTFS-plus`_ fare inputs are similar to GTFS fare inputs but with additional fare periods for time period-based fares.

However, since the columns `route_id`, `origin_id`, `destination_id` and `contains_id` are all optional in
`fare_rules.txt <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_rules.md>`_` and
therefore may be specified in different combinations, fast-trips implements fares with the following rules:

- ``contains_id`` is not implemented in Fast-Trips, and its inclusion will result in an error
- Specifying ``origin_id`` and not ``destination_id`` or vice versa will result in an error.  Each fare rule must
  specify both or neither.
- These combinations of ``route_id``, ``origin_id``, and `d`estination_id`` will be used to match a ``fare_id`` to a
  transit trip, in this order. The first match will win.
  - Matching ``route_id``, ``origin_id`` and ``destination_id``
  - Matching ``route_id`` only (no `origin_id` or ``destination_id`` specified)
  - Matching ``origin_id`` and `destination_id` only (no ``route_id`` specified)
  - No match (e.g. ``fare_id`` specified with no other columns)

Discount and free transfers specified in
`fare_transfer_rules_ft.txt <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_transfer_rules_ft.md>`_
are applied to transfers from one fare period to another fare period, and these links need to be *back-to-back*.  So if
a passenger transfers from A to B to C and the discount is specified for fare period A to fare period C, they will not
receive the discount.

Free transfers are also specified *within* fare periods (possibly time-bounded) in
`fare_attributes_ft.txt <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_attributes_ft.md>`_.
These free transfers are applied *after* the discounts from
`fare_transfer_rules_ft.txt <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_transfer_rules_ft.md>`_
and they do not need to be back-to-back.  So if a passenger transfers from A to B to A and fare period A has 1 free
transfer specified, but a transfer from B to A has a transfer fare of $.50, the passenger will receive the free transfer
since these rules are applied last (and override).

There are four places where fares factor into fast-trips.

1. During path-finding (C++ extension), fares get assessed as a cost onto links, which translate to generalized cost
(minutes) via the traveler's value of time.
`Fare transfer rules <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_transfer_rules_ft.md>`_
here are complicated, because we don't know which is the next/previous fare, and we can only guess based on
probabilities.  The fare is estimated using [`Hyperlink::getFareWithTransfer()`](src/hyperlink.cpp).

   Free transfers as configured in
   `fare attributes <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_attributes_ft.md>`_
   are implemented here in a simplistic way; that is, a free transfer is assumed if the fare attributes have granted any
   free transfers without looking at `transfer_duration` or the number of transfers. Also, this transfer is required to
   be *back-to-back* also.  A future enhancement could include keeping a transfer count for each fare period so that the
   back-to-back requirement is not imposed, and also so that a certain number of free fares could be tallied, but at
   this time, a simpler approach is used because it's not clear if this kind of detail is helpful.

   Turn this off using configuration option `transfer_fare_ignore_pathfinding`.

2. During path-enumeration (C++ extension), when the paths are being constructed by choosing links from the hyperpath
   graph, at the point where each link is added to the path, the
   `fare transfer rules <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_transfer_rules_ft.md>`_
   are applied to adjust fares with more certainty of the the path so far.  This is done in
   [`Hyperlink::setupProbabilities()`](src/hyperlink.cpp) which calls `Hyperlink::updateFare()` and updates the link
   cost as well if the fare is affected.  Free transfers as configured in
   `fare attributes <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_attributes_ft.md>`_
   are looked at here as well, but without the transfer duration component.

3. During path-enumeration (C++ extension), after the path is constructed, the trip cost is re-calculated at the end
   using [`Path::calculateCost()`](src/path.cpp).  At this moment in the process, the path is complete and final, so the
   fare transfer rules are relatively easy to apply given that links are certain.  The initial fare and cost are saved
   and passed back to python to show the effect of step 1.

   Free transfers as configured in
   `fare attributes <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/fare_attributes_ft.md>`_
   are also addressed here.

   Turn this off using configuration option `transfer_fare_ignore_pathenum`.

4. During simulation (python), while the path is being adjusted due to vehicle times, the fares are calculated via
   [`Route.add_fares()`](fasttrips/Route.py).  This is unlikely to change anything unless the fare periods changed due
   to the slow-down of vehicles -- so consider deprecating this in favor of using the pathfinding results?  For now,
   it's a good test that the C++ code is working as expected; running with simulation off should result in identical
   fare and cost results from pathfinding and the (non-vehicle-updating) python simulation.



.. _config_files:
Configuration Files
---------------------

There are two required configuration files:
 *  ``pathweights_ft.txt``  : weights assigned to each component of a transit path
 *  ``config_ft.txt`` : system, run setup and pathfinding configurations

An optional third configuration file:
  * ``config_ft.py`` : defines user classes in python.

.. _pathweights:
Pathweights Specification
^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``pathweight_ft.txt`` file is a *required* file that tells Fast-Trips how much to value each attribute of a path.
This will be used for the stop-labeling stage but also the path selection, which is done in a logit model.  Therefore,
the weights should be consistent with with utility.

A good rule of thumb to consider is that typical in-vehicle-time coefficients for mode choice logit models range from
0.01 to 0.08.  If you consider route choice to be a nest of mode choice, you would divide whatever the in-vehicle-time
coefficient is for mode choice by whatever that nesting coefficient is.  One assumption is that the nesting coefficient
for route choice should have a smaller value than a typical mode choice model, meaning that people are more likely to
switch routes than modes. So, if a mode-choice utility coefficient for in-vehicle time is 0.02 and an assumed nesting
coefficient is 0.2, the value for route choice would be 0.10 (0.02 / 0.2).

The file can be a csv or fixed-format.  If you use a fixed-format, make sure
`pathweights_fixed_width = True` in the run configuration file (e.g., `config_ft.txt`).


``pathweights_ft.txt`` **must** have the following columns:

+----------------------+-------+--------------------------------------+
| Column Name          | Type  | Description                          |
+======================+=======+======================================+
| ``user_class``       | Str   | Config functions can use trip list,  |
|                      |       | person, and household attributes to  |
|                      |       | return a user class string to the    |
|                      |       | trip.                                |
|                      |       |                                      |
|                      |       | The string that is returned          |
|                      |       | determines the set of path weights   |
|                      |       | that are used.                       |
+----------------------+-------+--------------------------------------+
| ``demand_mode_type`` | Str   | One of:                              |
|                      |       |  - ``transfer``                      |
|                      |       |  - ``access``                        |
|                      |       |  - ``egress``                        |
|                      |       |  - ``transit``                       |
+----------------------+-------+--------------------------------------+
| ``demand_mode``      | Str   | One of:                              |
|                      |       | - ``transfer``                       |
|                      |       | - a string specified as              |
|                      |       |   **access/egress mode** in          |
|                      |       |   ``trip_list.txt``demand file       |
|                      |       |   (i.e. ``walk``, ``PNR``)           |
|                      |       | - a string specified as a **transit  |
|                      |       |   mode** in ``trip_list.txt``demand  |
|                      |       |   file (i.e. ''local_bus`` )         |
+----------------------+-------+--------------------------------------+
| ``supply_mode``      | Str   | For ``demand_mode_type=transit``,    |
|                      |       | corresponds to the transit mode      |
|                      |       | as defined in the `gtfs_plus`_      |
|                      |       |                                      |
|                      |       | For ``demand_mode_type=transfer``,   |
|                      |       | one of:                              |
|                      |       |  - ``walk``                          |
|                      |       |  - ``wait``                          |
|                      |       |  - ``transfer_penalty``              |
|                      |       |                                      |
|                      |       | For ``demand_mode_type=access``,     |
|                      |       | one of:                              |
|                      |       |  - ``walk_access``                   |
|                      |       |  - ``pnr_access``                    |
|                      |       |  - ``bike_access``                   |
|                      |       |  - ``knr_access``                    |
|                      |       |                                      |
|                      |       | For ``demand_mode_type=egress``,     |
|                      |       | one of:                              |
|                      |       |  - ``walk_egress``                   |
|                      |       |  - ``pnr_egress``                    |
|                      |       |  - ``bike_egress``                   |
|                      |       |  - ``knr_egress``                    |
+----------------------+-------+--------------------------------------+
| ``weight_name``      | Str   | An attribute of the path link. See   |
|                      |       | below for more details.              |
+----------------------+-------+--------------------------------------+
| ``weight_value``     | Float |  The multiplier for the attribute    |
|                      |       |  named ``weight_name``               |
+----------------------+-------+--------------------------------------+

The following is an example of a minimally specified ``pathweight_ft.txt`` :

+----------------------+-------------------+-----------------+------------------------+------------------+
| *demand_mode_type*   | *demand_mode*     | *supply_mode*   | *weight_name*          | *weight_value*   |
+======================+===================+=================+========================+==================+
| ``access``           | ``walk``          | ``walk_access`` | ``time_min``           | .02              |
+----------------------+-------------------+-----------------+------------------------+------------------+
| ``egress``           | ``walk``          | ``walk_egress`` | ``time_min``           | .02              |
+----------------------+-------------------+-----------------+------------------------+------------------+
| ``transit``          | ``transit``       | ``local_bus``   | ``wait_time_min``      | .02              |
+----------------------+-------------------+-----------------+------------------------+------------------+
| ``transit``          | ``transit``       | ``local_bus``   | ``in_vehicle_time_min``| .01              |
+----------------------+-------------------+-----------------+------------------------+------------------+
| ``transfer``         | ``transfer``      | ``transfer``    | ``transfer_penalty``   | .05              |
+----------------------+-------------------+-----------------+------------------------+------------------+
| ``transfer``         | ``transfer``      | ``transfer``    | ``time_min``           | .02              |
+----------------------+-------------------+-----------------+------------------------+------------------+


.. _supply_modes_and_weights:
Determining supply modes and weight values
""""""""""""""""""""""""""""""""""""""""""""""""""""""

If a supply mode exists in ``pathweight_ft.txt``, it is assumed to be a valid mode to use for the associated demand mode.
 - Demand modes for each person are determined from each component of ``mode`` in ``trip_list.txt`` plus an implied
   *transfer*.
 - If the trip list were to specify that someone takes `commuter_rail`, then they can still take a local bus or any
   supporting mode on their trip in addition to commuter rail so long as it is specified in ``pathweight_ft.txt``.
 - If for some reason a supply mode  (i.e. ``rocket_ship``) *shouldn't* be used for a particular demand mode
   (i.e. ``land_based_transit``), then don't put a row with both of them there.

Weight values should make sense relative to each other
 - Weights are often assumed to be higher for "supportive" modes and lower for "main" modes to induce them to select a
   path with the selected demand mode, as in the example below.

+-------------------+-----------------+-------------------------+------------------+
| *demand_mode*     | *supply_mode*   | *weight_name*           | *weight_value*   |
+===================+=================+=========================+==================+
| ``commuter_rail`` | ``local_bus``   | ``in_vehicle_time_min`` | 0.015            |
+-------------------+-----------------+-------------------------+------------------+
| ``commuter_rail`` | ``heavy_rail``  | ``in_vehicle_time_min`` | 0.01             |
+-------------------+-----------------+-------------------------+------------------+
| ``local_bus``     | ``local_bus``   | ``in_vehicle_time_min`` | 0.01             |
+-------------------+-----------------+-------------------------+------------------+

Weight values should have appropriate meaning w.r.t. path choice context.
 - If a logit model is being used to select which path a traveler selects, the weights need to be scaled to be
   appropriate to that context.
 - Based on work summarized in NCHRP Report 716 (http://www.trb.org/Publications/Blurbs/167055.aspx), values for
   in-vehicle-travel-time for **mode choice** range from 0.01 to 0.05 per minute of travel
 - By assuming that path choice is a nested logit of a mode choice model, one can divide these values by a reasonable
   nesting parameter (ranging from ~0.2-0.8) to get a rough reasonable range of 0.01 to 0.20.

Weight Names
""""""""""""""""""

The column ``weight_name`` must conform to a set of constraints as discussed below.
 - For most of the weights prefix mode is not needed. E.g. there is no need to label ``weight_name`` ``time_min`` for
   ``supply_mode`` ``walk_access`` as ``walk_time_min``, because the fact that the ``supply_mode`` is ``walk_access``
   means it is only assessed on walk links.
 - The drive option (PNR/KNR access/egress), however, should have `walk_` and `drive_` prefixes, because the access can
   have both components: driving to the station from the origin and walking from the lot to the station. So for example,
   for ``supply_mode`` ``pnr_access`` there will be two weights associated with travel time: ``walk_time_min`` and
   ``drive_time_min``.


The following is a partial list of possible weight names based on the demand mode / supply mode combinations.

+-------------------+-----------------+-------------------------+------------------------+
| *demand_mode_type*| *demand_mode*   | *supply_mode*           | *weight names*         |
+===================+=================+=========================+========================+
| ``access``        | ``walk``        | ``walk_access``         | ``time_min``           |
|                   |                 |                         | ``depart_early_min``   |
|                   |                 |                         | ``depart_late_min``    |
+-------------------+-----------------+-------------------------+------------------------+
| ``egress``        | ``walk``        | ``walk_egress``         | ``time_min``           |
|                   |                 |                         | ``arrive_early_min``   |
|                   |                 |                         | ``arrive_late_min``    |
+-------------------+-----------------+-------------------------+------------------------+
| ``access``        | ``PNR``         | ``pnr_access``          | ``walk_time_min``      |
|                   |                 |                         | ``drive_time_min``     |
|                   |                 |                         | ``arrive_early_min``   |
|                   |                 |                         | ``arrive_late_min``    |
+-------------------+-----------------+-------------------------+------------------------+
| ``transfer``      | ``transfer``    | ``transfer``            | ``transfer_penalty``   |
|                   |                 |                         | ``time_min``           |
|                   |                 |                         | ``wait_time_min        |
+-------------------+-----------------+-------------------------+------------------------+
| ``transit``       | ``transit``     |                         | ``in_vehicle_time_min``|
|                   |                 |                         | ``wait_time_min``      |
+-------------------+-----------------+-------------------------+------------------------+

.. note::
  Note that the cost component is handled at the path level using the value of time column in ``trip_list.txt``.

.. _weightqualifiers:

Weight Qualifiers
""""""""""""""""""
By default, Fast-Trips will apply all weights as a constant on the appropriate variable. Fast-Trips also supports weight
qualifiers which allow for the weights to be applied using more complex models. The supported qualifiers are listed
below. Certain qualifiers also require modifiers to shape the cost function.

If no qualifier is specified, ``constant`` will be assumed.


+-------------------------+----------------------------------------------------------------------+--------------------+
| Qualifier               | Formulation                                                          | Required Modifiers |
+=========================+======================================================================+====================+
| ``constant`` (default)  | :math:`f(x) = weight * x`                                            | N/A                |
+-------------------------+----------------------------------------------------------------------+--------------------+
| ``exponential``         | :math:`f(x) = { (1 + weight) }^{x}`                                  | N/A                |
+-------------------------+----------------------------------------------------------------------+--------------------+
| ``logarithmic``         | :math:`f(x) = weight*{log_{base}}*x`                                 | ``log_base``       |
+-------------------------+----------------------------------------------------------------------+--------------------+
| ``logistic``            | :math:`f(x) = \frac{logistic\_max}{1+e^{-weight*(x-sigmoid)}}`       | ``logistic_max``   |
|                         |                                                                      | ``logistic_mid``   |
+-------------------------+----------------------------------------------------------------------+--------------------+

*Example*::

  #Pathweights_ft.txt snippet
  user_class purpose demand_mode_type demand_mode    supply_mode  weight_name                                   weight_value
  # default constant
  all        other   transit          transit        rapid_bus    wait_time_min                                 1.77

  # Explicitly constant
  all        other   transit          transit        rapid_bus    wait_time_min.constant                        1.77

  all        other   access           walk           walk_access  depart_early_min.logistic                     0.2
  all        other   access           walk           walk_access  depart_early_min.logistic.logistic_max        10
  all        other   access           walk           walk_access  depart_early_min.logistic.logistic_mid        9

  all        other   egress           walk           walk_egress  arrive_late_min.logarithmic                   0.3
  all        other   egress           walk           walk_egress  arrive_late_min.logarithmic.log_base          2.71828

  # Exponential
  all        work    access           walk           walk_access  depart_early_min.exponential                  0.02

  # Logarithmic
  all        other   egress           walk           walk_egress  arrive_late_min.logarithmic                   0.3
  all        other   egress           walk           walk_egress  arrive_late_min.logarithmic.log_base          2.71828

.. _configft:

Config_ft File
^^^^^^^^^^^^^^^^^^^^^^^^^^

``config_ft.txt`` is a *required* file whose location is specified at runtime.
If the same options are specified in both, then the version specified in the Transit Demand input directory will be used.
(Two versions may be specified because some configuration options are more relevant to demand and some are more relevant
to network inputs.)

The configuration files are parsed by python's
`ConfigParser module` <https://docs.python.org/2/library/configparser.html#module-ConfigParser>`_ and therefore
adhere to that format, with two possible sections: *fasttrips* and *pathfinding*.

Configuration Options: fasttrips
"""""""""""""""""""""""""""""""""""""

+---------------------------------------+--------+---------+----------------------------------------------+
| Option Name                           | Type   | Default | Description                                  |
+=======================================+========+=========+==============================================+
| ``bump_buffer``                       | float  | 5       | Not really used yet.                         |
+---------------------------------------+--------+---------+----------------------------------------------+
| ``bump_one_at_a_time``                | bool   | False   |                                              |
+---------------------------------------+--------+---------+----------------------------------------------+
| ``capacity_constraint``               | bool   | False   | Hard capacity constraint.  When True,        |
|                                       |        |         | fasttrips forces everyone off overcapacity   |
|                                       |        |         | vehicles and disallows them from finding     |
|                                       |        |         | a new path using an overcapacity vehicle.    |
+---------------------------------------+--------+---------+----------------------------------------------+
| ``create_skims``                      | bool   | False   | Run skimming after assignment.               |
+---------------------------------------+--------+---------+----------------------------------------------+
| ``debug_num_trips``                   | int    | -1      | If positive, will truncate the trip list     |
|                                       |        |         | to this length.                              |
+---------------------------------------+--------+---------+----------------------------------------------+
| ``debug_trace_only``                  | bool   | False   | If True, will only find paths and simulate   |
|                                       |        |         | the person ids specified in                  |
|                                       |        |         | ``trace_person_ids``                         |
+---------------------------------------+--------+---------+----------------------------------------------+
| ``debug_output_columns``              | bool   | False   | If True, will write internal & debug columns |
|                                       |        |         | into output.                                 |
+---------------------------------------+--------+---------+----------------------------------------------+
| ``fare_zone_symmetry``                | bool   | False   | If True, will assume fare zone symmetry.     |
|                                       |        |         | That is, if fare_id X is configured from     |
|                                       |        |         | origin zone A to destination zone B and      |
|                                       |        |         | there is no fare configured from zone B to   |
|                                       |        |         | zone A,  we'll assume that fare_id X         |
|                                       |        |         | also applies.                                |
+---------------------------------------+--------+---------+----------------------------------------------+
| ``max_iterations``                    | int    | 1       | Maximum number of pathfinding iterations     |
|                                       |        |         | to run.                                      |
+---------------------------------------+--------+---------+----------------------------------------------+
| ``number_of_processes``               | int    | 0       | Number of processes to use for path finding. |
+---------------------------------------+--------+---------+----------------------------------------------+
| ``output_passenger_trajectories``     | bool   | True    | Write chosen passenger paths?                |
|                                       |        |         | ##TODO: deprecate.                           |
|                                       |        |         | Why would you ever not do this?              |
+---------------------------------------+--------+---------+----------------------------------------------+
| ``output_pathset_per_sim_iter``       | bool   | False   | Output pathsets for each simulation          |
|                                       |        |         | iteration?  If false, just outputs once      |
|                                       |        |         | per path-finding iteration.                  |
+---------------------------------------+--------+---------+----------------------------------------------+
| ``prepend_route_id_to_trip_id``       | bool   | False   | This is for readability in debugging;        |
|                                       |        |         | If True, then route ids will be prepended    |
|                                       |        |         | to trip ids.                                 |
+---------------------------------------+--------+---------+----------------------------------------------+
| ``simulation``                        | bool   | True    | Simulate transit vehicles?                   |
|                                       |        |         | After path-finding, should fast-trips        |
|                                       |        |         | update vehicle times and put passengers      |
|                                       |        |         | on vehicles?                                 |
|                                       |        |         | If False, fast-trips:                        |
|                                       |        |         | - still calculates costs                     |
|                                       |        |         | and probabilities and chooses paths,         |
|                                       |        |         | - doesn't update vehicle times               |
|                                       |        |         | from those read in from the input network,   |
|                                       |        |         | - doesn't load passengers onto vehicles      |
|                                       |        |         | This is useful for debugging path-finding    |
|                                       |        |         | and verifying that pathfinding calculations  |
|                                       |        |         | are consisten twith cost/fare calculations   |
|                                       |        |         | done outside of pathfinding.                 |
+---------------------------------------+--------+---------+----------------------------------------------+
| ``skip_person_ids``                   | string | 'None'  | A list of person IDs to skip.                |
+---------------------------------------+--------+---------+----------------------------------------------+
| ``trace_ids``                         | string | 'None'  | A list of tuples, (person ID, person trip ID)|
|                                       |        |         | for whom to output verbose trace information.|
+---------------------------------------+--------+---------+----------------------------------------------+

Configuration Options: pathfinding
"""""""""""""""""""""""""""""""""""""""""""""""""""

+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| *Option Name*                           | *Type*   | *Default*             | *Description*                                 |
+=========================================+==========+=======================+===============================================+
| ``max_num_paths``                       | int      | -1                    | If positive, drops paths after this number of |
|                                         |          |                       | paths is reached IF probability               |
|                                         |          |                       | is less than ``min_path_probability``         |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``min_path_probability``                | float    | 0.005                 | Paths with probability less than this get     |
|                                         |          |                       | dropped IF ``max_num_paths`` specified AND    |
|                                         |          |                       | exceeded.                                     |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``min_transfer_penalty``                | float    | 0.1                   | Minimum transfer penalty. Safeguard against   |
|                                         |          |                       | having no transfer penalty which can result in|
|                                         |          |                       | terrible paths with excessive transfers.      |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``overlap_chunk_size``                  | int      | 500                   | How many person's trips to process at a time  |
|                                         |          |                       | in overlap calculations in python simulation  |
|                                         |          |                       | (more means faster but more memory required.) |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``overlap_scale_parameter``             | float    | 1                     | Scale parameter for overlap path size         |
|                                         |          |                       | variable.                                     |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``overlap_split_transit``               | bool     | False                 | For overlap calcs, split transit leg into     |
|                                         |          |                       | component legs (A to E becauses A-B-C-D-E)    |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``overlap_variable``                    | string   | ``count``             | The variable upon which to base the overlap   |
|                                         |          |                       | path size variable.  Can be:                  |
|                                         |          |                       |  - ``None``                                   |
|                                         |          |                       |  - ``count``                                  |
|                                         |          |                       |  - ``distance``                               |
|                                         |          |                       |  - ``time``                                   |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``pathfinding_type``                    | string   | ``stochastic``        | Pathfinding method.  Can be:                  |
|                                         |          |                       |  - ``deterministic``                          |
|                                         |          |                       |  - ``file``                                   |
|                                         |          |                       |  - ``stochastic``                             |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``pathweights_fixed_width``             | bool     | False                 | If true, read the pathweights file as a fixed |
|                                         |          |                       | width, left-justified table (as opposed to    |
|                                         |          |                       | a CSV, which is the default).                 |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``stochastic_dispersion``               | float    | 1.0                   | Stochastic dispersion parameter.              |
|                                         |          |                       | TODO: document this further.                  |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``stochastic_max_stop_process_count``   | int      | -1                    | In path-finding, how many times should we     |
|                                         |          |                       | process a stop during labeling?  Specify -1   |
|                                         |          |                       | for no max.                                   |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``stochastic_pathset_size``             | int      | 1000                  | In path-finding, how many paths (not          |
|                                         |          |                       | necessarily unique) determine a pathset?      |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``time_window``                         | float    | 30                    | In path-finding, the max time a passenger     |
|                                         |          |                       | would wait at a stop.                         |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``utils_conversion_factor``             | float    | 1.0                   | In the path-finding labeling stage, multiplies|
|                                         |          |                       | the utility by this factor to prevent negative|
|                                         |          |                       | costs.                                        |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``transfer_fare_ignore_pathfinding``    | bool     | False                 | In path-finding, suppress trying to adjust    |
|                                         |          |                       | fares using transfer rules. For performance.  |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``transfer_fare_ignore_pathenum``       | bool     | False                 | In path-enumeration, suppress trying to adjust|
|                                         |          |                       | fares using transfer rules. For performance.  |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``user_class_function``                 | string   | ``generic_user_class``| A function to generate a user class string    |
|                                         |          |                       | given a user record.                          |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``depart_early_allowed_min``            | float    | 0.0                   | Allow passengers to depart before their       |
|                                         |          |                       | departure time time target by this many       |
|                                         |          |                       | minutes                                       |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+
| ``arrive_late_allowed_min``             | float    | 0.0                   | Allow passengers to arrive after their arrival|
|                                         |          |                       | time target by this many minutes.             |
+-----------------------------------------+----------+-----------------------+-----------------------------------------------+



More on Overlap Path Size Penalties
""""""""""""""""""""""""""""""""""""""""""""

The path size overlap penalty is formulated by Ramming and discussed in Hoogendoorn-Lanser et al. (see
[References](#references) ).

When the pathsize overlap is penalized (pathfinding ``overlap_variable`` is not `None`), then the following equation is
used to calculate the path size overlap penalty:

:math:`PS_i = \sum_{a\in\Gamma_i}\frac{l_a}{L_i}*\frac{1}{\sum_{j\in C_{in}} \left(\frac{L_i}{L_j}\right)^\gamma*\delta_{aj}}`

Where
  - *i* is the path alternative for individual *n*
  - :math:`\Gamma_i` is the set of legs of path alternative *i*
  - :math:`l_a`  is the value of the ``overlap_variable`` for leg *a*.  So it is either 1, the distance or the time of leg *a* depending of if ``overlap_scale_parameter`` is ``count``, ``distance`` or ``time``, respectively.
  - :math:`L_i` is the total sum of the ``overlap_variable`` over all legs :math:`l_a` that make up path alternative *i*
  - :math:`C_{in}`  is the choice set of path alternatives for individual *n* that overlap with alternative *i*
  - :math:`\gamma` is the ``overlap_scale_parameter``
  - :math:`\delta_{ai} = 1\ and\ \delta_{aj} = 0\ \forall\ j\ \ne i`

From Hoogendoor-Lanser et al.:

  Consequently, if leg *a* for alternative *i* is unique, then
   - the denominator is equal to 1 and
   - the path size contribution of leg *a* is equal to its proportional length :math:`\frac{l_a}{L_i}`

  If leg *l<sub>a</sub>* is also used by alternative *j*, then:
   - the contribution of leg :math:`l_a` to path size :math:`PS_i` is smaller than :math:`\frac{l_a}{L_i}`

  If :math:`\gamma = 0` or if routes *i* and *j* have equal length, then
   - the contribution of leg *a* to :math:`PS_i` is equal to :math:`\frac{l_a}{2L_i}`

  If :math:`\gamma > 0` and routes *i* and *j* differ in length, then
   - the contribution of leg *a* to :math:`PS_i` depends on the ratio of :math:`L_i` to :math:`L_j`.

  If route *i* is longer than route *j*
   - and :math:`\gamma > 1`, then
    - the contribution of leg *a* to :math:`PS_i` is larger than :math:`\frac{l_a}{2L_i}`
   - otherwise,
    - the contribution is smaller than :math:`\frac{l_a}{2L_i}`.

  If :math:`\gamma > 1` in the exponential path size formulation, then
   - long routes are penalized in favor of short routes.

  If overlapping routes have more or less equal length, then
   - The use of parameter :math:`\gamma` is questionable and should therefore be set to 0.
   - Overlap between those alternatives should not affect their choice probabilities differently.
   - The degree to which long routes should be penalized might be determined by estimating :math:`\gamma`.
   - If :math:`\gamma` is not estimated, then an educated guess with respect to :math:`\gamma` should be made.
   - To this end, differences in route length between alternatives in a choice set should be considered.

User Class Configuration: config_ft.py
""""""""""""""""""""""""""""""""""""""""""""

``config_ft.py`` is an *optional* python file containing functions that are evaluated to ascertain items such as user
classes.
This could be used to programmatically define user classes based on person, household and/or trip attributes.

The function name for user class is specified in the *pathfinding* input parameter ``user_class_function``

*Example:*::

  def user_class(row_series):
      """
      Defines the user class for this trip list.

      This function takes a single argument, the pandas.Series with person, household and
      trip_list attributes, and returns a user class string.
      """
      if row_series["hh_id"].lower() in ["simpson","brady","addams","jetsons","flintstones"]:
          return "fictional"
      return "real"


.. _skim_class_file:
Skim_classes_ft File
^^^^^^^^^^^^^^^^^^^^^^^^^^

If skimming is turned on in :ref:`Configuration Options: fasttrips`, then fasttrips requires a file which specifies
the combination of parameters for which skims are sought, like time periods, access/egress modes, etc.
The file must be in csv format and for each line, a separate skim is generated. Note that there are no default
values so each parameter must be specified on each line. The following columns are required.

+-----------------------+--------+-------------------------------------------------------------------------+
| *Parameter Name*      | *Type* |  *Description*                                                          |
+=======================+========+=========================================================================+
| ``start_time``        | int    | Start of skimming period in minutes after midnight                      |
+-----------------------+------- +-------------------------------------------------------------------------+
| ``end_time``          | int    | End of skimming period in minutes after midnight                        |
+-----------------------+------- +-------------------------------------------------------------------------+
| ``sampling_interval`` | int    | Sample frequency for skim path building in minutes. This means the      |
|                       |        | number of skim path building runs is                                    |
|                       |        | (time_period_end - time_period_start) / time_period_sampling_interval.  |
+-----------------------+------- +-------------------------------------------------------------------------+
| ``vot``               | float  | Value of time for skim calculation.                                     |
+-----------------------+------- +-------------------------------------------------------------------------+
| ``purpose``           | float  | Trip purpose.                                                           |
+-----------------------+------- +-------------------------------------------------------------------------+
| ``access_mode``       | float  | Access mode used to access PT services.                                 |
+-----------------------+------- +-------------------------------------------------------------------------+
| ``transit_mode``      | float  | Transit demand mode (see below table).                                  |
+-----------------------+------- +-------------------------------------------------------------------------+
| ``egress_mode``       | float  | Egress mode used to reach destination from final PT stop.               |
+-----------------------+------- +-------------------------------------------------------------------------+


Note that the modes are demand modes as defined in :ref:`passenger_demand`; see also
:ref:`supply_modes_and_weights`.




.. _output_files:


.. _dynopath_based_output:
Passenger Path Output
------------------

Fast-Trips uses the `dyno_path`_ data standard to convey sets of paths, or a `pathset`.  Each Path is comprised of a set
of links. Each dyno-path pathset is comprised of two sets of files, a path-file, and a link file, described in the
following sections.

``enumerated_links.csv`` and ``enumerated_paths.csv``
  Paths that are enumerated after the path-finding/labeling step.
``pathset_links.csv`` and ``pathset_paths.csv``
  Paths that are considered by passengers in the path choice process.
``chosen_links.csv`` and ``chosen_paths.csv``
  Paths that are selected by passengers.

Path files
^^^^^^^^^^^^^

Path-based output files depict the available and enumerated paths in the path choice set in the `dyno_path`_ format.

Dyno-path path file **required** attributes:

+--------------------+--------------------------------------------------------------------------------+
| *variable*         | *Description*                                                                  |
+====================+================================================================================+
| ``person_id``      | Corresponds to person_id field in dyno-demand-formatted demand                 |
+--------------------+--------------------------------------------------------------------------------+
|``trip_list_id_num``| Corresponds to line number field in dyno-demand-formatted trip_list.txt where  |
|                    | 1 is the first trip. To be replaced when dyno-demand issue#2 is resolved.      |
+--------------------+--------------------------------------------------------------------------------+
| ``pathdir``        | Direction. 1 for outbound, 2 for inbound.                                      |
+--------------------+--------------------------------------------------------------------------------+
| ``pathmode``       | Demand mode. Corresponds to mode field in dyno-demand-formatted `trip_list.txt`|
+--------------------+--------------------------------------------------------------------------------+

Dyno-path Path file optional attributes

+--------------------+--------------------------------------------------------------------------------+
| *variable*         | *description*                                                                  |
+====================+================================================================================+
| ``pf_iteration``   | Path-finding iteration.                                                        |
+--------------------+--------------------------------------------------------------------------------+
| ``pathnum``        | ID within a pathset.                                                           |
+--------------------+--------------------------------------------------------------------------------+
| ``pf_cost``        | Debug. The generalized cost as calculated by the path finder.                  |
+--------------------+--------------------------------------------------------------------------------+
| ``pf_probability`` | Debug. The probability of the path as calculated by the path finder.           |
+--------------------+--------------------------------------------------------------------------------+
| ``description``    | Text description of the path, including all nodes and links.                   |
+--------------------+--------------------------------------------------------------------------------+
| ``chosen``         | Chosen status for path. -1 if not chosen, -2 if chosen but rejected, otherwise |
|                    | iteration + simulation_iteration/100.                                          |
+--------------------+--------------------------------------------------------------------------------+
| ``missed_xfer``    | 1 if the path has a missed transfer.                                           |
+--------------------+--------------------------------------------------------------------------------+
| ``sim_cost``       | Generalized cost calculated in the assignment/simulation.                      |
+--------------------+--------------------------------------------------------------------------------+
|``logsum_component``| Debug. Portion of the total logsum from this path.                             |
+--------------------+--------------------------------------------------------------------------------+
| ``logsum``         | Debug. Total logsum for the pathset.                                           |
+--------------------+--------------------------------------------------------------------------------+
| ``probability``    | Debug. Probability of this path as calculated by the route choice model.       |
+--------------------+--------------------------------------------------------------------------------+
| ``iteration``      | Iteration in which this path was found.                                        |
+--------------------+--------------------------------------------------------------------------------+

Link files
^^^^^^^^^^^^

Link-based dyno-path output files depict the links within the available and enumerated paths in the path choice set in
the `dyno_path`_ format.

Dyno-path link file **required** Attributes

+----------------+--------------------------------------------------------------------------------+
| *variable*     | *description*                                                                  |
+================+================================================================================+
| ``person_id``  | Corresponds to `person_id` field in dyno-demand-formatted demand               |
+----------------+--------------------------------------------------------------------------------+
| ``p-trip_id``  | Corresponds to `p-trip_id `in dyno-demand-formatted `trip_list.txt`.           |
|                | Unique within the household/person.                                            |
+----------------+--------------------------------------------------------------------------------+
| ``link_num``   | The integer link/path segment number representing the order that this link     |
|                | takes place in the entire path                                                 |
+----------------+--------------------------------------------------------------------------------+
| ``A_id``       | Starting node for link / path segment. Can be a stop_id corresponding to       |
|                | `stops.txt` or a taz corresponding to an access link such as                   |
|                | `walk_access_ft.txt`                                                           |
+----------------+--------------------------------------------------------------------------------+
| ``B_id``       | Ending node for link / path segment. Can be a `stop_id` corresponding to       |
|                | `stops.txt` or a taz corresponding to an access link such as                   |
|                | `walk_access_ft.txt`                                                           |
+----------------+--------------------------------------------------------------------------------+
| ``mode``       | Supply mode for the link, corresponds to mode in GTFS-PLUS-formatted           |
|                | `routes_ft.txt` or an access or egress mode.                                   |
+----------------+--------------------------------------------------------------------------------+
| ``link_mode``  | One of: [ `access` , `egress` , `transfer` , `transit` ]                       |
+----------------+--------------------------------------------------------------------------------+
| ``trip_id``    | Transit trip ID for the trip, corresponding to trip_id in GTFS-PLUS-formatted  |
|                | trips.txt                                                                      |
+----------------+--------------------------------------------------------------------------------+
| ``route_id``   | Transit route short name corresponding to route_id variables in                |
|                | GTFS-PLUS-formatted route_ft.txt                                               |
+----------------+--------------------------------------------------------------------------------+

Dyno-path link file **optional** Attributes that aren't Fast-Trips specific:

+-----------------+--------------------------------------------------------------------------------+
| *variable*      | *description*                                                                  |
+=================+================================================================================+
| ``pathnum``     | ID within a pathset.                                                           |
+-----------------+--------------------------------------------------------------------------------+
| ``A_time``      | Time at start node accounting for dwell delays. In fast-trips, it is based on  |
|                 | ``pf_A_time`` but adjusted due to dwell delays.                                |
+-----------------+--------------------------------------------------------------------------------+
| ``B_time`       | Time at end node accounting for dwell delays. In fast-trips, it is based on    |
|                 | ``pf_B_time`` but adjusted due to dwell delays.                                |
+-----------------+--------------------------------------------------------------------------------+
| ``wait_time``   | Wait time in minutes accounting for dwell time. In fast-trips, it is based on  |
|                 | ``pf_wait_time`` at the start node and adjusted based on difference between    |
|                 | ``A_time`` and ``pf_A_time``.                                                  |
+-----------------+--------------------------------------------------------------------------------+
| ``board_time``  | Time passenger boards a transit vehicle (as opposed to arriving at the         |
|                 | start node) accounting for dwell time.                                         |
+-----------------+--------------------------------------------------------------------------------+
| ``alight_time`` | Time passenger alights from the transit vehicle accounting for dwell time.     |
+-----------------+--------------------------------------------------------------------------------+
| ``link_time``   | Link time in minutes accounting for dwell times. In fast-trips,it is based on  |
|                 | ``pf_link_time`` but adjusted for dwell times.                                 |
+-----------------+--------------------------------------------------------------------------------+
| ``A_seq``       | Stop sequence for the starting node of the link, corresponding to              |
|                 | ``stop_sequence`` in GTFS-PLUS-formatted ``stop_times.txt``                    |
+-----------------+--------------------------------------------------------------------------------+
| ``B_seq``       | Stop sequence for the ending node of the link, corresponding to                |
|                 | ``stop_sequence`` in GTFS-PLUS-formatted ``stop_times.txt``                    |
+-----------------+--------------------------------------------------------------------------------+
| ``sim_cost``    | Generalized cost calculated in the assignment/simulation.                      |
+-----------------+--------------------------------------------------------------------------------+
| ``missed_xfer`` | 1 if the transfer is missed. (This happens if ``new_waittime`` is negative.)   |
+-----------------+--------------------------------------------------------------------------------+
| ``chosen``      | Chosen status for path.                                                        |
|                 |  - -1 if not chosen by passenger                                               |
|                 |  - -2 if chosen but passenger rejected because of capacity or timing issues    |
|                 |  - otherwise: +_/100.                                                          |
+-----------------+--------------------------------------------------------------------------------+
| ``overcap``     | Number of passengers overcap for the transit vehicle for this link.            |
+-----------------+--------------------------------------------------------------------------------+
| ``overcap_frac``| Fraction of attempted boards that are overcapacity at this stop.               |
+-----------------+--------------------------------------------------------------------------------+
| ``iteration``   | Iteration corresponding to this pathset.                                       |
+-----------------+--------------------------------------------------------------------------------+

Dyno-path link file with **optional, dubug- and internal fast-trips** Attributes:

+----------------------+--------------------------------------------------------------------------------+
| *variable*           | *description*                                                                  |
+======================+================================================================================+
| ``A_id_num``         | Numeric version of A_id, which could be a stop_id and taz.                     |
+----------------------+--------------------------------------------------------------------------------+
| ``B_id_num``         | Numeric version of B_id, which could be a stop_id or taz.                      |
+----------------------+--------------------------------------------------------------------------------+
| ``mode_num``         | Numeric version of mode.                                                       |
+----------------------+--------------------------------------------------------------------------------+
| ``trip_id_num``      | Numeric version of trip_id.                                                    |
+----------------------+--------------------------------------------------------------------------------+
| ``pf_iteration``     | Path-finding iteration.                                                        |
+----------------------+--------------------------------------------------------------------------------+
| ``pf_A_time``        | The time at the start node when used by the path-finding algorithm.            |
+----------------------+--------------------------------------------------------------------------------+
| ``pf_B_time``        | The time at the end node when used by the path-finding algorithm.              |
+----------------------+--------------------------------------------------------------------------------+
| ``pf_link_time``     | The link time in minutes when used by the path-finding algorithm.              |
+----------------------+--------------------------------------------------------------------------------+
| ``pf_wait_time``     | The wait time in minutes at the start node when used by the path-finding       |
|                      | algorithm.                                                                     |
+----------------------+--------------------------------------------------------------------------------+
| ``bump_iter``        | Iteration a passenger was bumped.                                              |
+----------------------+--------------------------------------------------------------------------------+
| ``bump_stop-boarded``|  1 means this passenger boarded, 0 means got bumped.                           |
+----------------------+--------------------------------------------------------------------------------+
| ``alight_delay_min`` | Delay in alight time from the input path-finding understanding of              |
|                      | alight time due to changes in dwell time.                                      |
+----------------------+--------------------------------------------------------------------------------+

.. _vehicle_based_output

Vehicle Based Output
-----------------------

``veh_trips.csv``
  Contains a record for each *vehicle-trip, stop* and *iteration, pathfinding_iteration, simulation_iteration*
  combination.

  Vehicle-based output depicts ridership by transit vehicle.  Eventually it will be translated into the `gtfs_ride`_
  data standard.

+-------------------------+--------------------------------------------------------------------------------+
| *Variable*              | *Description*                                                                  |
+=========================+================================================================================+
|``iteration``            | global fast-trips iteration                                                    |
+-------------------------+--------------------------------------------------------------------------------+
|``pathfinding_iteration``| pathfinding iteration                                                          |
+-------------------------+--------------------------------------------------------------------------------+
|``simulation_iteration`` | simulation iteration                                                           |
+-------------------------+--------------------------------------------------------------------------------+
|``direction_id``         | 0 or 1, as coded in ``trips.txt`` in `GTFS_PLUS`_                              |
+-------------------------+--------------------------------------------------------------------------------+
|``service_id``           | As coded in ``trips.txt`` in `GTFS_PLUS`_                                      |
+-------------------------+--------------------------------------------------------------------------------+
|``route_id``             | As coded in ``trips.txt`` in `GTFS_PLUS`_                                      |
+-------------------------+--------------------------------------------------------------------------------+
|``trip_id``              | As coded in ``trips.txt`` in `GTFS_PLUS`_                                      |
+-------------------------+--------------------------------------------------------------------------------+
|``stop_sequence``        | As coded in ``stop_times.txt`` in `GTFS_PLUS`_                                 |
+-------------------------+--------------------------------------------------------------------------------+
|``stop_id``              | As coded in ``stop_times.txt`` in `GTFS_PLUS`_                                 |
+-------------------------+--------------------------------------------------------------------------------+
|``arrival_time``         | As coded in ``stop_times.txt`` in `GTFS_PLUS`_                                 |
+-------------------------+--------------------------------------------------------------------------------+
|``arrival_time_min``     | As coded in ``stop_times.txt`` in `GTFS_PLUS`_                                 |
+-------------------------+--------------------------------------------------------------------------------+
|``departure_time``       | As coded in ``stop_times.txt`` in `GTFS_PLUS`_                                 |
+-------------------------+--------------------------------------------------------------------------------+
|``departure_time_min``   | As coded in ``stop_times.txt`` in `GTFS_PLUS`_                                 |
+-------------------------+--------------------------------------------------------------------------------+
|``travel_time_sec``      | Travel time from previous stop, as coded in ``stop_times.txt`` in `GTFS_PLUS`_ |
+-------------------------+--------------------------------------------------------------------------------+
|``dwell_time_sec``       | Dwell time for stop, calculated based on ``dwell_formula`` equation in         |
|                         | ``vehicles_ft.txt`` in `GTFS_PLUS`_                                            |
+-------------------------+--------------------------------------------------------------------------------+
|``capacity``             | Passengers that can be on board the vehicle, per ``vehicles_ft.txt`` in        |
|                         | `GTFS_PLUS`_                                                                   |
+-------------------------+--------------------------------------------------------------------------------+
|``boards``               | Passengers boarding at stop, per Fast-Trips.                                   |
+-------------------------+--------------------------------------------------------------------------------+
|``alights``              | Passengers alighting at stop, per Fast-Trips.                                  |
+-------------------------+--------------------------------------------------------------------------------+
|``onboard``              | Passengers on-board vehicle as it approaches the stop.                         |
+-------------------------+--------------------------------------------------------------------------------+
|``standees``             | Standees on vehicle as it approaches the stop.                                 |
+-------------------------+--------------------------------------------------------------------------------+
|``friction``             | ``boards``+``alights``+``standees``.  Can be used in dwell time calculations.  |
+-------------------------+--------------------------------------------------------------------------------+
|``overcap``              | Initializes at -1, then ``onboard``- ``capacity``                              |
+-------------------------+--------------------------------------------------------------------------------+


.. _trace_output
Trace  Output
------------------

Output from person traces is currently contained in a very lengthy .log file as well as specially labeled link- and path-
output csvs in the `dyno_path`_ formats.

``output_trace_<trace_label>.log``
  A comprehensive debug log of every calculation for the specified rider.

``fasttrips_labels_<trace_label>_<iteration>.csv``
  Pathfinding for specified rider with link info.  Has a row for the A and B node of each link for each label iteration.

  +-----------------------+-------------------------------------------------------+
  | *Variable*            | *Description*                                         |
  +=======================+=======================================================+
  | ``label_iter``        | Stop labeling iteration; each iteration updates       |
  |                       | another set of labels emanating from current node     |
  +-----------------------+-------------------------------------------------------+
  | ``link``              | link #, starting from 1                               |
  +-----------------------+-------------------------------------------------------+
  | ``node ID``           | node id, which is either the start or end of the link |
  +-----------------------+-------------------------------------------------------+
  | ``time``              | cumulative time passed based on least cost label [?]  |
  +-----------------------+-------------------------------------------------------+
  | ``mode``              | Link supply mode.                                     |
  +-----------------------+-------------------------------------------------------+
  | ``trip_id``           | If a transit link, ``trip_id`` from `GTFS_PLUS`_.     |
  |                       | Else, non-motorized mode type.                        |
  +-----------------------+-------------------------------------------------------+
  | ``link_time``         | Time of the specific link.                            |
  +-----------------------+-------------------------------------------------------+
  | ``link_cost``         | Cost of the specific link.                            |
  +-----------------------+-------------------------------------------------------+
  | ``cost``              | Cumulative composit label (logsum) of the node.       |
  +-----------------------+-------------------------------------------------------+

``fasttrips_labels_ids_<trace_label>_<iteration>.csv``
  Pathfinding for specified rider, for debugging.

  +-----------------------+-------------------------------------------------------+
  | *Variable*            | *Description*                                         |
  +=======================+=======================================================+
  | ``stop_id``           | the stop id per `GTFS_PLUS`_                          |
  +-----------------------+-------------------------------------------------------+
  | ``stop_id_label_iter``| Stop labeling iteration; each iteration updates       |
  |                       | another label                                         |
  +-----------------------+-------------------------------------------------------+
  | ``is_trip``           | boolean; did the labeling algorithm reach this stop   |
  |                       | via a transit trip, or a non-motorized link?          |
  |                       | The algorithm alternates between the two.             |
  +-----------------------+-------------------------------------------------------+
  | ``label_stop_cost``   | Cost given to that stop for that iteration based on   |
  |                       | the cost of the previous stop and the link used to get|
  |                       | here                                                  |
  +-----------------------+-------------------------------------------------------+

.. _computing_performance_output
Computing Performance Output
-----------------------------

``ft_output_performance.csv``
  Outputs start- and end- time and memory for each iteration of each step of fast-trips: read_configuration, pathfinding,
  assignment, simulation iteration, and output.  Note that mid-process memory is not able to be logged :-(

``ft_output_performance_pathfinding.csv``
  Detailed output for each trip on path finding performance. Includes
   - process number,
   - pathfinding ieration,
   - number of paths labelled,
   - if it was traced,
   - the label iterations it took,
   - the max times each stop was processed
   - the time it took in clock hours and seconds
   - time it took enumerating in clock hours and seconds
   - memory usage and time of memory timestamp

.. _settings_output
Settings Output
-----------------------------

``ft_output_config.txt``
  Just in case you threw away any record of the settings you used to run Fast-Trips, the input files you used, ...or if you wanted to know what settings Fast-Trips actually used when you gave it multiple layers of direction, you can review them here.


Skimming Output
---------------
``skims/skim_index_to_zone_id_mapping.csv``
  Mapping from 0-based skim index (first column) to arbitrary zone identifiers as specified in network files (second
  column). Note that this information is also stored as an attribute in the omx file, however it is not specified as an
  omx mapping because the identifiers can be arbitrary strings (like in the Springfield example) and types that
  cannot be cast to integers `are not supported by omx <https://github
  .com/osPlanning/omx-python/blob/337ea4deff0c0055a6e792a5bf45aabf3fc82075/openmatrix/File.py#L284>`_ currently.


``skims/{user_class}_{purpose}_{access}_{transit}_{egress}_{vot}/{skim_name}.omx``
  For each requested combination of (user_class, purpose, access_mode, transit_mode, egress_mode, vot) there is a
  corresponding sub-directory which contains all skims. All components are saved to individual omx files, with the file
  name a combination of component_name, skim start time, skim end time, and skim sampling interval.
