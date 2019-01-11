#!/usr/bin/env python

import pandas as pd
import json
from pandas.io.json import json_normalize
from sodapy import Socrata
from sqlalchemy import *

def get_data():
    client = Socrata("data.baltimorecity.gov", 'TMhF3Ib2mVhj6Mt7X8Y9mSIj9')
    dat_json = client.datasets()
    with open('data/metadata.json', 'w') as outfile:
        json.dump(dat_json, outfile)
    return(dat_json)

def split_normalize(json, by):
    out = []
    for data in json:
        data_i = data[by]
        data_i['id'] = data['resource']['id']
        data_i['permalink'] = data['permalink']
        data_i['link'] = data['link']
        out.append(data_i)
    table = json_normalize(out)
    return(table)

if __name__ == "__main__":
    dat_json = get_data()
    resource = split_normalize(dat_json, 'resource')
    engine = create_engine('mysql+pymysql://healyt22:l@Xiscool22@localhost/baltimore')
    resource.to_sql('metadata_resource', con=engine)
