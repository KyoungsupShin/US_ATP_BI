import pandas as pd
import numpy as np
from datetime import datetime
import re
import time
import itertools
import os
import xlrd
import urllib
from datetime import timedelta

class Manipluations():
    def yyyymmdd_datetime(self, x):
        if len(x) == 0: #fully string data ex) TBD
            return ''
        if pd.isna(x): # nan value filter
            return '' 
        try:
            if pd.isnull(np.datetime64(x)): #NAT value filter
                return ''    
        except:
            pass
        x = str(x)
        removal_string = ['/', '.', '-', '_', ' ']    
        for rs in removal_string:
            x = x.replace(rs, '')
        if len(x) > 8:
            x = x[:8]
        try:
            return datetime.strftime(datetime.strptime(x, '%m%d%Y'), '%Y-%m-%d')
        except:
            try:
                return datetime.strftime(datetime.strptime(x, '%Y%m%d'), '%Y-%m-%d')
            except:
                return ''
                pass
            pass

    def data_int(self, x):
        if pd.isna(x):
            return 0
        elif (type(x) != int) or (type(x) != float):
            try:
                return int(x)
            except:
                return 0 
                pass
        else:
            return int(x)

    def data_float(self, x):
        try:
            if type(x) == str:
                x = re.findall(r'\d+', x)
                if len(x) == 0:
                    return 0
                else:
                    return float(x)
            elif type(x) == int:
                return float(x)
            elif pd.isna(x):
                return 0
            else: 
                return x
        except:
            return 0
            pass

    def data_text(self, x):
        if pd.isna(x):
            return ''
        else:
            x = str(x)
            # x = x.replace(u'\xa0', u' ')
            x = x.strip()
            x = x.replace("'", "")
            if len(x) > 150:
                return x[:100]
            else:
                return x
