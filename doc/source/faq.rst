
FAQ
==================

How do I restart a run after pathfinding?
  Use the option ``pathfinding_type=file``, via  ``scripts/runTest.py`` or in the configuration.  Then, drop the
  ``pathsfound_paths.csv`` and ``pathsfound_links.csv`` files in the output directory for your run, and they'll be read
  in instead of generated.
