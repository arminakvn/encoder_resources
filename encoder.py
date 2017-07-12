
# coding: utf-8

# In[105]:

import pandas as pd
from pandas import DataFrame
import googlemaps
import geocoder
from sqlalchemy import create_engine
from sqlalchemy import text as sc_text
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import sys, requests, json, urllib, shutil, datetime, os, time, csv, types,requests


# In[106]:

target_fields_lookup = [
        dict({
                'dataset_name': 'all_parcels',
                'addr_str': 'addr_str',
                'site_addr': 'site_addr',
                'muni_id': 'muni_id',
                'muni': 'muni',
                'addr_zip':'addr_zip',
                'ma_lp_id': 'ma_lp_id',
                'addr_num': 'addr_num',
                'luc': 'luc_1',
                'tiger_id': 'tiger_id'
            }),
        dict({
                'dataset_name': 'all_parcels_16',
                'addr_str': 'street_1',
                'muni_id': 'muni_id',
                'muni': 'muni',
                'addr_zip':'zip',
                'ma_lp_id': 'Land_Parcel_ID',
                'tiger_id': 'TLID_1',
                'addr_num': 'minNum_1',
                'luc': 'LU',
                'yr_built':'YR_BUILT',
                'site_addr': 'site_addr'

            })

    ]


# In[107]:

tds=1


# In[108]:

engine = create_engine("postgresql://postgres:@128.31.25.188:5432")
NumberTypes = (types.IntType, types.LongType, types.FloatType, types.ComplexType)


# In[109]:

def getStandardAddress(addr):
    lower_case_address = addr.lower()
    apt_units = ["apt", "unit", "no", "no.", "apartment"]
    for unit in apt_units:
        if unit in addr:
            substr_index = addr.find(unit)
            if substr_index < 1:
                spltd = addr.split()
                if spltd[0] == unit:
                    # means the unit number is spaced so makit it together
                    joind_unit = "".join((spltd[0],spltd[1]))
                    spltd.pop(0)
                    spltd.pop(0)
                    back_in = [joind_unit] + spltd
                    # now joing them back
                    fixed_addr = " ".join(back_in)
                else:
                    fixed_addr = addr
            else:
                # it s somewhere in the middle and needs to go in begining
                # is it attached like apt4 and not apt 4
                spltd = addr.split()
                for each_sub in spltd:
                    if unit in each_sub:
                        sub_index = spltd.index(each_sub)
                if spltd[sub_index] == unit: 
                    unit_type = spltd[sub_index]
                    spltd.pop(sub_index)
                    unit_num = spltd[sub_index]
                    spltd.pop(sub_index)
                    joind_unit = "".join([unit_type,unit_num])
                    back_in = [joind_unit] + spltd
                    # now joing them back
                    fixed_addr = " ".join(back_in)                        
    try:
        
        print "final address",fixed_addr
    except: 
        fixed_addr = addr
    sql = "SELECT house_num, name, pretype, extra,predir,sufdir,building,qual, suftype, city, country, state, postcode, unit FROM standardize_address('us_lex','us_gaz', 'us_rules','{}'); "
    engine = create_engine("postgresql://postgres:@128.31.25.188:5432/network")
    df = pd.read_sql_query(sc_text(sql.format(fixed_addr)),con=engine)
    print "resultd: ", df
    return df



def makeMuniWalk():
    print "before making the muni walk"
    df = pd.read_sql_query('select distinct muni,muni_id from "all_parcels"',con=engine)
    dgr = df.groupby("muni_id",sort=True,as_index=False).first()
    dgr_munis = dgr["muni"].tolist()
    dgr_muni_ids = dgr["muni_id"].tolist()
    muniwalk = dict()
    for i in range(0, len(dgr_munis)-1):
        muniwalk.update({"{0}".format(dgr_munis[i].upper()): dgr_muni_ids[i]})    
    return muniwalk


def extractMuniID(muniname):
    print "going for figureing the muni name for "
    muniwalk = makeMuniWalk()
    if isinstance(muniname, basestring) and len:
        return muniwalk[muniname.upper()]
    else: 
        return ""

def extractMuniName(addre):
    print "will try to extract the name for "
    __df = pd.read_sql_query('select distinct muni from "all_parcels"',con=engine)
#    print "__df", __df
    dgr_munis = __df["muni"].tolist()
    uppermunis = [muni.upper() for muni in dgr_munis]
    muni_name = ""
    if len(addre.split(" "))>1:
        for each_comp in addre.split(" "):
#            print "component that is muni", each_comp
            if each_comp.upper() in uppermunis:
                muni_name = muni_name + each_comp
    return muni_name


def extractAddrNum(addre):
    if len(addre.split(" "))>1:
        return addre.split(" ")[0] 
    else:
        ""



def extractZipCodeNum(addre):
    if len(addre.split(" "))>1:
        return addre.split(" ")[len(addre.split(" "))-1] 
    else:
        ""

def fuzzeGeoCode(*args, **keyvals):    
    street_string = keyvals["street_string"]
    choices = keyvals["choices"]
    precision = keyvals["precision"]
    ndf = keyvals["ddf"]
    query = street_string#.replace("str","").replace("str","")
#    print "in fuzzy itself, ", street_string, precision, query
    extr = process.extract(query, choices)
#    print "extracted results ", extr
    if extr[0][1] > precision:
        extr_in_choices = choices.index(extr[0][0])
        return extr_in_choices #ndf.iloc[extr_in_choices,:]["ma_lp_id"]
    else:
        
        return ""



def addressFormat(address_row,city_row,zipcode_row):
    url = "https://geocoding.geo.census.gov/geocoder/geographies/address?street={0}&city={1}&state=MA&benchmark=Public_AR_Census2010&vintage=Census2010_Census2010&layers=14&format=json".format(address_row, city_row)
#    print url
    r = requests.get(url)
    if r.status_code !=200:
        print "limit"
        limited = True
        return ""
#    print "request address to census", r
    try:
        current = json.dumps(r.json())
        current = json.loads(current)
#        print current["result"]#["addressMatches"]#[0]["tigerLine"]["tigerLineId"]
        return current["result"]#["addressMatches"]#[0]["addressComponents"]
    except Exception as e:
#        print "nomatch"
        return "nomatch"

def processRow(target_dataset,address_str, street_name,zipcode_str,muni, addr_num,precision,engine):
    """ In ths process row: 
        we get zip code to furthur filter the choices and make the list
    """
    neighborhoods = ["BRIGHTON", "DORCHESTER","WEST ROXBURY",                                  "ROXBURY","CHARLSTOWN","ROSLINDALE","JAMAICA PLAIN","ALLSTON",                                 "SOUTH BOSTON","EAST BOSTON","MATTAPAN","HAYDE PARK","MISSION HILL"]
    if muni in neighborhoods:
        muni = "BOSTON"
#    print "muni", muni
    
    bsql = ['select * from "all_parcels" where muni=:muni','select * from "all_parcels_16" where muni=:muni'][tds]
    bdf = pd.read_sql_query(sc_text(bsql.format("%")),con=engine,params={'muni': muni})

    if tds==1:
        bdf["zip"] = bdf.apply(lambda row: "0{0}".format(row[zipcode_field_name]),axis=1) 
        
    
    cdf = bdf.loc[bdf[target_fields_lookup[tds]["addr_zip"]]==zipcode_str,:]
    
    # make choices
    if len(cdf) == 0:
        cdf = bdf#.copy()

    ddf = cdf#.copy()
    def fixNumber(num):
        return "{0}".format(str(num).replace(".0",""))
    
                            
                            
    def makeAddrStr(num,street):
        return "{0} {1}".format(str(num).replace(".0",""),street)
    if tds==1:
        ddf["site_addr"] = ddf.apply(lambda row: makeAddrStr(row["minNum_1"],row["street_1"]),axis=1)
        ddf["addr_num"] = ddf.apply(lambda row: fixNumber(row["minNum_1"]),axis=1)
    # try to find directly:
#     try:
    street_df = ddf.loc[ddf["street_1"]==street_name,]
    
#     print "street found df", street_df
    addres_numb_fnd =street_df.loc[street_df["addr_num"]==addr_num,]
    if len(addres_numb_fnd) > 0:
#         print " address found and row is",addres_numb_fnd
        return addres_numb_fnd#["Land_Parcel_ID"]
    else:   
        # compare without 
        choices = ddf["site_addr"].tolist()
#         print "choices are:", choices
        extr_in_choices = fuzzeGeoCode(street_string=address_str, choices=choices , precision=precision, muni=muni, zipcode_str=zipcode_str,ddf=ddf)
#         print "extr_in choices returnd buy fizzy ginc, which is the index",extr_in_choices
        if isinstance(extr_in_choices, basestring) and len(extr_in_choices) > 0:
            ma_lp_id = ddf.iloc[extr_in_choices,:]#["ma_lp_id"]
#             print "found the ma_lp_id", ma_lp_id
        elif isinstance(extr_in_choices, NumberTypes) and len:
            ma_lp_id = ddf.iloc[extr_in_choices,:]#["ma_lp_id"]
#             print "found the ma_lp_id", ma_lp_id
        else:
#             print "zero found"
            ma_lp_id = "zero found"
        return ma_lp_id

def processGeocodeCall(inputaddressframe,engine):
    
    
    # clean up the unit / apt / issue
    # see if there is apt in it and not unit
    
    
    row = inputaddressframe.copy().iloc[0,]
#    print row
    returndDf = processRow("all_parcels","{0} {1}".format(row["name"],row["suftype"]),row["name"], row["postcode"],row["city"], row["house_num"],75,engine)
#    print "processed row", returndDf
#     inputaddressframe["ma_lp_id"] = inputaddressframe.apply(lambda row: processRow("all_parcels","{0} {1}".format(row["name"],row["suftype"]),row["name"], row["postcode"],row["city"], row["house_num"],95), axis=1)

    return returndDf


# In[110]:

# df = pd.read_csv("addresstest.csv")

# address_list = df["Address"].tolist()

# for each_addres in address_list:
#     _df_add = getStandardAddress("{0}".format(each_addres))


# In[111]:

def googleGeoCode(address_row):
    print "google geocode"
    API_KEY = "AIzaSyCt2msG_ry8rcQLhycMY6LkdNxeKoctuug"
    address_formated = dict()
    url = "https://maps.googleapis.com/maps/api/geocode/json?address={0}&key={1}".format(address_row, API_KEY)
    print "url"
    r = requests.get(url)
    if r.status_code !=200:
        print "limit"
        limited = True
        return ""
    current = json.dumps(r.json())
    current = json.loads(current)
    for each_component in current["results"][0]["address_components"]:
        if "street_number" in each_component["types"]:
            address_formated["street_number"] = each_component["long_name"]
        elif "postal_code" in each_component["types"]:
            address_formated["postalcode"] = each_component["long_name"]
        elif "route" in each_component["types"]:
            address_formated["route"] = each_component["long_name"]
        elif "administrative_area_level_2" in each_component["types"]:
            address_formated["county"] = each_component["long_name"]
        elif "locality" in each_component["types"]:
            address_formated["municipality"] = each_component["long_name"]
        elif "administrative_area_level_1" in each_component["types"]:
            address_formated["state"] = each_component["long_name"]
    return address_formated


# In[112]:


class EnCoderAgent(object):
    """
        geocoder that gets address/lists and geocodes/encodes 
        with geocode/spatial/other type of info
        returns better complete parcel information
    """
    fix_by_osm = True
    def __init__(self, *args, **keyvals):
        self.tds = 1

        print "init"
    
    
    def saveAsStandardAddress(self,addr):
        lower_case_address = addr.lower()
        apt_units = ["apt", "unit", "no", "no.", "apartment"]
        for unit in apt_units:
            if unit in addr:
                substr_index = addr.find(unit)
                if substr_index < 1:
                    spltd = addr.split()
                    if spltd[0] == unit:
                        # means the unit number is spaced so makit it together
                        joind_unit = "".join((spltd[0],spltd[1]))
                        spltd.pop(0)
                        spltd.pop(0)
                        back_in = [joind_unit] + spltd
                        # now joing them back
                        fixed_addr = " ".join(back_in)
                    else:
                        fixed_addr = addr
                else:
                    # it s somewhere in the middle and needs to go in begining
                    # is it attached like apt4 and not apt 4
                    spltd = addr.split()
                    for each_sub in spltd:
                        if unit in each_sub:
                            sub_index = spltd.index(each_sub)
                    if spltd[sub_index] == unit: 
                        unit_type = spltd[sub_index]
                        spltd.pop(sub_index)
                        unit_num = spltd[sub_index]
                        spltd.pop(sub_index)
                        joind_unit = "".join([unit_type,unit_num])
                        back_in = [joind_unit] + spltd
                        # now joing them back
                        fixed_addr = " ".join(back_in)                        
        try:

            print "final address"
        except: 
            fixed_addr = addr
        split_num = fixed_addr.tolist()[0].split()
#        print "split num", split_num
#        df['house_num'] = df['house_num'].apply(clean_string_to_list)

#        print "df",df['house_num']
        sql = "SELECT house_num, name, pretype, extra,predir,sufdir,building,qual, suftype, city, country, state, postcode, unit FROM standardize_address('us_lex','us_gaz', 'us_rules','{}'); "
        engine = create_engine("postgresql://postgres:@128.31.25.188:5432/network")
        df = pd.read_sql_query(sc_text(sql.format(fixed_addr)),con=engine)
#        print "resultd: ", df
        
#               print "split_num", split_num,dfcp
        return df


        
    def preProcess(self,address):
        
        spltd = address.split("-")
        # flag this as a possible plural and return the first one
        start_num = spltd[0]
        end_addre_spl = spltd[1]
        split_rest = end_addre_spl.split()
        end_num = split_rest[0]
        self.plural_address = {'type': 'range', 'range_start_end': [start_num,end_num]}
        # return the first number + rest of the address
        return_address = "{0}".format(end_addre_spl.replace("{}".format(end_num), "{}".format(start_num)))
        return return_address
            
    def prepare_results(self,processed_df):
        try:
            self._df_add["address_found"] = self.processed_df[target_fields_lookup[self.tds]["site_addr"]].tolist()[0]
            self._df_add["ma_lp_id"] = self.processed_df[target_fields_lookup[self.tds]["ma_lp_id"]].tolist()[0]
            self._df_add["luc"] = self.processed_df[target_fields_lookup[self.tds]["luc"]].tolist()[0]
            self._df_add["tiger_id"] = self.processed_df[target_fields_lookup[self.tds]["tiger_id"]].tolist()[0]
            self._df_add["yr_built"] = self.processed_df[target_fields_lookup[self.tds]["yr_built"]].tolist()[0]
            return self._df_add
        except:
#            print "proeessed df", processed_df
            self._df_add["address_found"] = self.processed_df[target_fields_lookup[self.tds]["site_addr"]]
            self._df_add["ma_lp_id"] = self.processed_df[target_fields_lookup[self.tds]["ma_lp_id"]]
            self._df_add["luc"] = self.processed_df[target_fields_lookup[self.tds]["luc"]]
            self._df_add["tiger_id"] = self.processed_df[target_fields_lookup[self.tds]["tiger_id"]]
            self._df_add["yr_built"] = self.processed_df[target_fields_lookup[self.tds]["yr_built"]]
            return self._df_add
    def process(self, address):
        fix_by_osm = True
        """check for address stuff like address numbers from-to"""
        if "-" in address:
            print "there is - in address, preprocessing"
            address = self.preProcess(address)
        self._df_add = getStandardAddress(address)
        engine = create_engine("postgresql://postgres:@128.31.25.188:5432/")
        self.processed_df = processGeocodeCall(self._df_add, engine)
        try:
            print "try: findin postgis"
            self._df_add = getStandardAddress(address)
            engine = create_engine("postgresql://postgres:@128.31.25.188:5432/")
            self.processed_df = processGeocodeCall(self._df_add, engine)
        except:
            if fix_by_osm:
                print "except:fix by osm"
                address_formated = geocoder.osm("{0}".format(address))
                address = "{0} {1}, {2} {3} {4}".format(address_formated.json["housenumber"],address_formated.json["street"],address_formated.json["city"],address_formated.json["state"],address_formated.json["postal"])
            
            else:
                print "exept: fix by bing"
                address_formated = geocoder.bing("{0}".format(address),key="ApdsLDh8yd-ma4xCpdY-qbmYvUl7D9br_qvXYiYzXDz1PQqS_2khXrc5gYAJt88y")
                print "address json"
                address=address_formated.json["address"]
#            print address
            self._df_add = getStandardAddress(address)
            engine = create_engine("postgresql://postgres:@128.31.25.188:5432/")
            self.processed_df = processGeocodeCall(self._df_add, engine)
        else:
            print "else"
        finally:
            return_df = self.prepare_results(self.processed_df)
        return return_df
    
    
    
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