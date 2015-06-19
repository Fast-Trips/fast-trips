:PSRC
:pax200_deterministic_iter1_nocap_PSRC
python scripts\runTest.py ..\FAST-TrIPS-1\Examples\PSRC\pax200_deterministic_iter1_nocap Examples\PSRC

:pax200_deterministic_iter1_cap1_PSRC
python scripts\runTest.py ..\FAST-TrIPS-1\Examples\PSRC\pax200_deterministic_iter1_cap1 Examples\PSRC

:pax200_deterministic_iter2_cap1_PSRC
python scripts\runTest.py ..\FAST-TrIPS-1\Examples\PSRC\pax200_deterministic_iter2_cap1 Examples\PSRC

:: stochastic
:pax100_stochastic_iter1_nocap_PSRC
python scripts\runTest.py ..\FAST-TrIPS-1\Examples\PSRC\pax200_stochastic_iter1_nocap Examples\PSRC

:pax100_stochastic_iter1_cap1_PSRC
python scripts\runTest.py ..\FAST-TrIPS-1\Examples\PSRC\pax200_stochastic_iter1_cap1 Examples\PSRC

:pax100_stochastic_iter2_cap1_PSRC
python scripts\runTest.py ..\FAST-TrIPS-1\Examples\PSRC\pax200_stochastic_iter2_cap1 Examples\PSRC

:: ------------------------------------------------------------------------------------------------

:SanFrancisco
:pax200_deterministic_iter1_nocap_SF
python scripts\runTest.py ..\FAST-TrIPS-1\Examples\SanFrancisco\pax200_deterministic_iter1_nocap Examples\SanFrancisco

:pax200_deterministic_iter1_cap1_SF
python scripts\runTest.py ..\FAST-TrIPS-1\Examples\SanFrancisco\pax200_deterministic_iter1_cap1 Examples\SanFrancisco

:pax200_deterministic_iter2_cap1_SF
python scripts\runTest.py ..\FAST-TrIPS-1\Examples\SanFrancisco\pax200_deterministic_iter2_cap1 Examples\SanFrancisco

:: stochastic
:pax100_stochastic_iter1_nocap_SF
python scripts\runTest.py ..\FAST-TrIPS-1\Examples\SanFrancisco\pax200_stochastic_iter1_nocap Examples\SanFrancisco

:pax100_stochastic_iter1_cap1_SF
python scripts\runTest.py ..\FAST-TrIPS-1\Examples\SanFrancisco\pax200_stochastic_iter1_cap1 Examples\SanFrancisco

:pax100_stochastic_iter2_cap1_SF
python scripts\runTest.py ..\FAST-TrIPS-1\Examples\SanFrancisco\pax200_stochastic_iter2_cap1 Examples\SanFrancisco


:: cprofile for snakeviz
python -m cProfile -o Examples\SanFrancisco\pax1000_deterministic_iter1_nocap\ft.prof scripts\runTest.py ..\FAST-TrIPS-1\Examples\SanFrancisco\pax1000_deterministic_iter1_nocap Examples\SanFrancisco


:done