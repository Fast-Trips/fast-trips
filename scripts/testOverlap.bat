::
:: Overlap Tests
::

:: first, setup for run
set NETWORK_DIR=sfcta\network_draft1.9
set DEMAND_DIR=sfcta\CHAMP_fasttrips_demand_v0.3
set TEST_SIZE=1000

:: goto overlap_split_test

:: run with pathfinding
python .\scripts\runTest.py --num_trips %TEST_SIZE% stochastic 1 "%NETWORK_DIR%" "%DEMAND_DIR%" sfcta
IF ERRORLEVEL 1 goto done

:overlap_test
:: run without pathfinding
for %%A in (None count distance time) do (

  rem run again without pathfinding
  mkdir "%DEMAND_DIR%_file_iter1_nocap"

  rem copy pathfinding files in place
  copy  "%DEMAND_DIR%_stochastic_iter1_nocap\pathsfound_paths.csv" "%DEMAND_DIR%_file_iter1_nocap"
  copy  "%DEMAND_DIR%_stochastic_iter1_nocap\pathsfound_links.csv" "%DEMAND_DIR%_file_iter1_nocap"

  rem do the test
  python .\scripts\runTest.py --num_trips %TEST_SIZE% --overlap_variable %%A file 1 "%NETWORK_DIR%" "%DEMAND_DIR%" sfcta
  IF ERRORLEVEL 1 goto done

  rem save results
  move "%DEMAND_DIR%_file_iter1_nocap" "%DEMAND_DIR%_file_iter1_nocap_overlap_%%A"
)

:: one more time but with split transit
:overlap_split_test
:: run without pathfinding
for %%A in (count distance time) do (

  rem run again without pathfinding
  mkdir "%DEMAND_DIR%_file_iter1_nocap"

  rem copy pathfinding files in place
  copy  "%DEMAND_DIR%_stochastic_iter1_nocap\pathsfound_paths.csv" "%DEMAND_DIR%_file_iter1_nocap"
  copy  "%DEMAND_DIR%_stochastic_iter1_nocap\pathsfound_links.csv" "%DEMAND_DIR%_file_iter1_nocap"

  rem do the test
  python .\scripts\runTest.py --num_trips %TEST_SIZE% --overlap_variable %%A --overlap_split_transit file 1 "%NETWORK_DIR%" "%DEMAND_DIR%" sfcta
  IF ERRORLEVEL 1 goto done

  rem save results
  move "%DEMAND_DIR%_file_iter1_nocap" "%DEMAND_DIR%_file_iter1_nocap_overlap_%%A_split"
)
:done