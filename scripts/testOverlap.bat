::
:: Overlap Tests
::

:: first, setup for run
set NETWORK_DIR=sfcta\network_draft1.8
set DEMAND_DIR=sfcta\CHTS_fasttrips_demand_v0.2

goto overlap_split_test

:: run with pathfinding
python .\scripts\runTest.py stochastic 1 "%NETWORK_DIR%" "%DEMAND_DIR%" sfcta
IF ERRORLEVEL 1 goto done

:overlap_test
:: run without pathfinding
for %%A in (None count distance time) do (

  rem run again without pathfinding
  mkdir sfcta\CHTS_fasttrips_demand_v0.2_file_iter1_nocap

  rem copy pathfinding files in place
  copy  sfcta\CHTS_fasttrips_demand_v0.2_stochastic_iter1_nocap\pathsfound_paths.csv sfcta\CHTS_fasttrips_demand_v0.2_file_iter1_nocap
  copy  sfcta\CHTS_fasttrips_demand_v0.2_stochastic_iter1_nocap\pathsfound_links.csv sfcta\CHTS_fasttrips_demand_v0.2_file_iter1_nocap

  rem do the test
  python .\scripts\runTest.py --overlap_variable %%A file 1 "%NETWORK_DIR%" "%DEMAND_DIR%" sfcta
  IF ERRORLEVEL 1 goto done

  rem save results
  move sfcta\CHTS_fasttrips_demand_v0.2_file_iter1_nocap sfcta\CHTS_fasttrips_demand_v0.2_file_iter1_nocap_overlap_%%A
)

:: one more time but with split transit
:overlap_split_test
:: run without pathfinding
for %%A in (count distance time) do (

  rem run again without pathfinding
  mkdir sfcta\CHTS_fasttrips_demand_v0.2_file_iter1_nocap

  rem copy pathfinding files in place
  copy  sfcta\CHTS_fasttrips_demand_v0.2_stochastic_iter1_nocap\pathsfound_paths.csv sfcta\CHTS_fasttrips_demand_v0.2_file_iter1_nocap
  copy  sfcta\CHTS_fasttrips_demand_v0.2_stochastic_iter1_nocap\pathsfound_links.csv sfcta\CHTS_fasttrips_demand_v0.2_file_iter1_nocap

  rem do the test
  python .\scripts\runTest.py --overlap_variable %%A --overlap_split_transit file 1 "%NETWORK_DIR%" "%DEMAND_DIR%" sfcta
  IF ERRORLEVEL 1 goto done

  rem save results
  move sfcta\CHTS_fasttrips_demand_v0.2_file_iter1_nocap sfcta\CHTS_fasttrips_demand_v0.2_file_iter1_nocap_overlap_%%A_split
)
:done