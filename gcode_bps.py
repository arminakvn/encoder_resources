# -*- coding: utf-8 -*-
"""
Created on Sun Mar 19 10:36:33 2017

@author: armin
"""

# In[]
import pandas as pd
from encoder import EnCoderAgent
# In[]
e = EnCoderAgent()
address_field_name = "ADD_LINE1"
zipcode_field_name = "ZIP_mmc"
muni_field_name = "CITY"
# In[]
#df = pd.read_excel("ENROLLMENT_ADDRESSES.xlsx").head()
#df.to_csv("ENROLLMENT_ADDRESSES.csv")
# In[]
df = pd.read_csv('ENROLLMENT_ADDRESSES.csv',skiprows=range(1, 60000),nrows=30)#, nrows=10) names=names,

# In[]
adr_grp = df.groupby(address_field_name, sort=True, as_index=False).first()

# In[]
results = list()
for each in adr_grp.iterrows():
    addr = each[1].ADD_LINE1
    try:
        res = e.process(addr)
        results.append(res)
        print "res", res
    except:
        results.append("fail")