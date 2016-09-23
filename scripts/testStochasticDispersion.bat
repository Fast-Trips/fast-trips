::
:: STochastic Dispersion Tests
::

:: first, setup for run
set NETWORK_DIR=sfcta\network_draft1.8
set DEMAND_DIR=sfcta\CHTS_fasttrips_demand_v0.2

for %%A in (1.0 0.9 0.8 0.7 0.6 0.5 0.4 0.3 0.2 0.1) do (

  python .\scripts\runTest.py --dispersion %%A stochastic 1 "%NETWORK_DIR%" "%DEMAND_DIR%" sfcta

  rem save results
  move sfcta\CHTS_fasttrips_demand_v0.2_stochastic_iter1_nocap sfcta\CHTS_fasttrips_demand_v0.2_stochastic_iter2_nocap_disp_%%A
)