::
:: Fare Tests
::

:: first, setup for run
set NETWORK_DIR=sfcta\network_draft1.9_fare
set DEMAND_DIR=sfcta\CHAMP_fasttrips_demand_v0.3
set TEST_SIZE=1000

:: run normally
python .\scripts\runTest.py --num_trips %TEST_SIZE% --output_dir output_fares stochastic 1 "%NETWORK_DIR%" "%DEMAND_DIR%" sfcta2\faretest
if ERRORLEVEL 1 goto done

:: create maps
python .\scripts\create_tableau_path_map.py --description "%NETWORK_DIR%" "sfcta2\faretest\output_fares"
if ERRORLEVEL 1 goto done

:: rename files
copy "sfcta2\faretest\output_fares\ft_output_config.txt"      sfcta2\faretest\ft_output_config_fares.txt
copy "sfcta2\faretest\output_fares\ft_output_performance.csv" sfcta2\faretest\ft_output_performance_fares.csv
copy "sfcta2\faretest\output_fares\pathset_links.csv"         sfcta2\faretest\pathset_links_fares.csv
copy "sfcta2\faretest\output_fares\pathset_paths.csv"         sfcta2\faretest\pathset_paths_fares.csv
copy "sfcta2\faretest\output_fares\pathset_map_points.csv"    sfcta2\faretest\pathset_map_points_fares.csv

::---------------------------------------------------------------------------------------------------------------------
:: no PF fares
python .\scripts\runTest.py --num_trips %TEST_SIZE% --transfer_fare_ignore_pathfinding --output_dir output_fares_nopf stochastic 1 "%NETWORK_DIR%" "%DEMAND_DIR%" sfcta2\faretest
if ERRORLEVEL 1 goto done

:: create maps
python .\scripts\create_tableau_path_map.py --description "%NETWORK_DIR%" "sfcta2\faretest\output_fares_nopf"
if ERRORLEVEL 1 goto done

:: rename files
copy "sfcta2\faretest\output_fares_nopf\ft_output_config.txt"      sfcta2\faretest\ft_output_config_fares_nopf.txt
copy "sfcta2\faretest\output_fares_nopf\ft_output_performance.csv" sfcta2\faretest\ft_output_performance_fares_nopf.csv
copy "sfcta2\faretest\output_fares_nopf\pathset_links.csv"         sfcta2\faretest\pathset_links_fares_nopf.csv
copy "sfcta2\faretest\output_fares_nopf\pathset_paths.csv"         sfcta2\faretest\pathset_paths_fares_nopf.csv
copy "sfcta2\faretest\output_fares_nopf\pathset_map_points.csv"    sfcta2\faretest\pathset_map_points_fares_nopf.csv

::---------------------------------------------------------------------------------------------------------------------
:: no PF no PE fares
python .\scripts\runTest.py --num_trips %TEST_SIZE% --transfer_fare_ignore_pathfinding --transfer_fare_ignore_pathenum  --output_dir output_fares_nopf_nope stochastic 1 "%NETWORK_DIR%" "%DEMAND_DIR%" sfcta2\faretest
if ERRORLEVEL 1 goto done

:: create maps
python .\scripts\create_tableau_path_map.py --description "%NETWORK_DIR%" "sfcta2\faretest\output_fares_nopf_nope"
if ERRORLEVEL 1 goto done

:: rename files
copy "sfcta2\faretest\output_fares_nopf_nope\ft_output_config.txt"      sfcta2\faretest\ft_output_config_fares_nopf_nope.txt
copy "sfcta2\faretest\output_fares_nopf_nope\ft_output_performance.csv" sfcta2\faretest\ft_output_performance_fares_nopf_nope.csv
copy "sfcta2\faretest\output_fares_nopf_nope\pathset_links.csv"         sfcta2\faretest\pathset_links_fares_nopf_nope.csv
copy "sfcta2\faretest\output_fares_nopf_nope\pathset_paths.csv"         sfcta2\faretest\pathset_paths_fares_nopf_nope.csv
copy "sfcta2\faretest\output_fares_nopf_nope\pathset_map_points.csv"    sfcta2\faretest\pathset_map_points_fares_nope.csv


:done


::---------------------------------------------------------------------------------------------------------------------
:: develop
:: git checkout develop
:: python .\scripts\runTest.py --num_trips %TEST_SIZE% --output_dir output_develop stochastic 1 "sfcta\network_draft1.9" "%DEMAND_DIR%" sfcta2\faretest
:: if ERRORLEVEL 1 goto done
:: 
:: :: create maps
:: python .\scripts\create_tableau_path_map.py --description "sfcta\network_draft1.9" "sfcta2\faretest\output_develop"
:: if ERRORLEVEL 1 goto done
:: 
:: :: rename files
:: copy "sfcta2\faretest\output_develop\ft_output_config.txt"      sfcta2\faretest\ft_output_config_develop.txt
:: copy "sfcta2\faretest\output_develop\ft_output_performance.csv" sfcta2\faretest\ft_output_performance_develop.csv
:: copy "sfcta2\faretest\output_develop\pathset_links.csv"         sfcta2\faretest\pathset_links_develop.csv
:: copy "sfcta2\faretest\output_develop\pathset_paths.csv"         sfcta2\faretest\pathset_paths_develop.csv
:: copy "sfcta2\faretest\output_develop\pathset_map_points.csv"    sfcta2\faretest\pathset_map_points_develop.csv
