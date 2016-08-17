:: test network with 2 paths (essentially)

:: deterministic, 2 iterations, capacity constraints on
python scripts\runTest.py deterministic 2 True Examples\test_network\input Examples\test_network\demand_twopaths Examples\test_network\output
if errorlevel 1 goto error

:: stochatic, 4 iterations, capacity constraint off
python scripts\runTest.py stochastic 4 False Examples\test_network\input Examples\test_network\demand_twopaths Examples\test_network\output
if errorlevel 1 goto error

:: stochatic, 4 iterations, capacity constraint on
python scripts\runTest.py stochastic 4 True Examples\test_network\input Examples\test_network\demand_twopaths Examples\test_network\output
if errorlevel 1 goto error

:: test network with regular demand

:: deterministic, 2 iterations, capacity constraints on
python scripts\runTest.py deterministic 2 True Examples\test_network\input Examples\test_network\demand_reg Examples\test_network\output
if errorlevel 1 goto error

:: stochatic, 2 iterations, capacity constraint off
python scripts\runTest.py stochastic 4 False Examples\test_network\input Examples\test_network\demand_reg Examples\test_network\output
if errorlevel 1 goto error

:: stochatic, 2 iterations, capacity constraint on
python scripts\runTest.py stochastic 2 True Examples\test_network\input Examples\test_network\demand_reg Examples\test_network\output
if errorlevel 1 goto error

:done
echo Completed without errors
goto :eof

:error
echo Bummer