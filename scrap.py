import quandl
import numpy as np


quandl.ApiConfig.api_key = 'ameENmRH1g3ztXhiv-zW'
quandl.ApiConfig.api_version = '2015-04-09'

ret = quandl.get("USTREASURY/YIELD", returns="numpy")


