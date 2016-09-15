::
:: Stochastic Max Stop Process Count Tests
::

:: first, setup for run
set NETWORK_DIR=sfcta\network_draft1.8
set DEMAND_DIR=sfcta\CHTS_fasttrips_demand_v0.2

for %%A in (10 50 100) do (

  python .\scripts\runTest.py --max_stop_process_count %%A stochastic 1 "%NETWORK_DIR%" "%DEMAND_DIR%" sfcta

  rem save results
  move sfcta\CHTS_fasttrips_demand_v0.2_stochastic_iter1_nocap sfcta\CHTS_fasttrips_demand_v0.2_stochastic_iter2_nocap_maxstopproc_%%A
)