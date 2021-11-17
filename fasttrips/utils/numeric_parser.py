from functools import partial
import pandas as pd

vparse_numeric = partial(pd.to_numeric, errors='raise')
