# Author:                       Eni Awowale & Coline Dony
# Date first written:           December 18, 2018
# Date last updated:            June 13, 2019
# Purpose:                      Extract BLS data

# Problem Statement:
# For several projects at the AAG, it would be valauble to utilize data
# made available by the Bureau of Labor Statistics (BLS) on jobs
# and occupations. This mai framework calls different modules that
# were developed to extract these data using the BLS API and re-format these
# data in different formats (spreadsheets, maps, etc.) as needed.
"""
    1.  PYTHON LIBRARIES
    all libraries necessary to run all scripts
"""
import os
import requests
import json

# Change main directory to our folder
folder = r'C:\Users\oawowale\Documents\GitHub\bls-and-geographers'
os.chdir(folder)

"""
    BUILDING BLOCKS TO CREATE, SEND, and GET THE RESPONSE OF A BLS QUERY
    see https://www.bls.gov/developers/api_signature_v2.htm#parameters
    A BLS QUERY is composed of:
    (A) the data we are requesting (in the form of a series_id) 
    (B) the start and end years we want to extract data from (optional)
    (C) whether or not we want to use an API key (optional)
    A BLS HTTP REQUEST needs to be composed of the following items:
    (D) the BLS API location (there are 2 locations: v1 and v2),
    (E) the data query (see C.)
    (F) the header, which contains what format we want to be sent
    (G) Using the "GET" function of an HTTP REQUEST the response can be read.
"""

# A. We are requesting "Occupational Employment Statistics" data under
# the the Employment and Unemployment Survey, which requires a specific format
# https://www.bls.gov/help/hlpforma.htm#OE
# Data Viewer app: https://beta.bls.gov/dataQuery/find?removeAll=1

# What a single series_id should be made of
prefix = 'OE'               # Occupational Employment
seasonal_code = 'U'         # Seasonal
area_type_code = 'S'        # Statewide
state_code = '01'           # 01 stands for Alabama for example
area_code = '00000'         # Statewide
industry_code = '000000'    # Cross-industry, Private, Federal, State, and Local Government
occupation_code = '193092'  # 19-3092 stands for geogrpaher for example
data_type = '01'            # Employment

# A1. State codes: read the states code dictionary from BLS website
# The BLS State codes represent each state and its name as used in their databases 
# see https://download.bls.gov/pub/time.series/sa/sa.state

BLS_dictionary_url = r'https://download.bls.gov/pub/time.series/sa/sa.state'
BLS_dictionary_textfile = requests.get(BLS_dictionary_url).text
bls_states_db = {}

for line in BLS_dictionary_textfile.split('\n')[1:]:
  try: state_code, state_name = line.strip().split('\t')
  except: continue
  bls_states_db[state_code] = state_name

# A2. Occupation codes: read occupation codes that relate to Geography occupation
# The AAG collects salaraies from the BLS website for geography-related occupations
# We use the AAG list of occupaiton

AAG_salary_data_textfilename = r'Salary Data 2018 updated.txt'
AAG_salary_data_textfile = open(AAG_salary_data_textfilename).readlines()
headers = AAG_salary_data_textfile[0].split('\t')
aag_occupations_db = {}

for line in AAG_salary_data_textfile[1:]:
       occupation, occupation_code = line.split('\t')[:2]
       if occupation_code != '':
              aag_occupations_db[occupation_code] = occupation

# A3. Create list of multiple series_ids
series_ids_file = open('list_series_id.txt', 'w')
for state_code in bls_states_db:
    for occupation_code_long in aag_occupations_db:
        occupation_code = occupation_code_long.replace('-', '').replace('.', '')[:6]
        series_id = prefix + seasonal_code + area_type_code + state_code + area_code + industry_code + occupation_code + data_type
        series_ids_file.write(series_id + '\n')
series_ids_file.close()

# A4. Split the list into chucks of 50 series ids
series_ids = [series_id.strip() for series_id in open('list_series_id.txt').readlines()]
series_ids_chunks = [series_ids[i:i+50] for i in range(0, len(series_ids), 50)]

# B. (Years) What years do we want to extract data from?
startyear = 2018
endyear = 2018

# C. Make Requests to the BLS API, using multiple series ids

# BLS API keys
#bls_api_key_Eni = 'de6366639eb64fa79045c9071a080dd5'
bls_api_key_Coline = '41d57752042240da84a71fd2ba7c748d'

# BLS API query

#I wanted to make sure my edits were working so I changed the chunk to 8
#the out put of text file shows two different states and how the state name changes depending
#depending on the state_code within the series_id
data_query = json.dumps({"seriesid": series_ids_chunks[8],
                         "startyear":startyear,
                         "endyear":endyear,
                         "registrationkey": bls_api_key_Coline})
#print(data_query)

# BLS API location
bls_api_location = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'

# Query Header: requests the response to be in the JSON format
headers = {'Content-type': 'application/json'}

# Send the query to the BLS API
post_request = requests.post(bls_api_location, data=data_query, headers=headers)

# Get the API response as text and convert it to a JSON dictionary
get_response = json.loads(post_request.text)
#print(get_response['Results']['series'][1])

series_ids_value_textfile = open('series_id_value.txt', 'w')
#created a new column for the state names in format tl_2018_us_state data shapefile
series_ids_value_textfile.write('Name\tSeries ID\tTotal Employment\n')

for series in get_response['Results']['series']:
    series_id = series['seriesID']
    try: employment = series['data'][0]['value']
    except: employment = 'n/a'
    #for loop for looking at state_codes in bls_states_db
    for state_code in bls_states_db:
        #this is the index of the series_id that holds the state_code information
        if state_code == series_id[4:6]:
            series_ids_value_textfile.write( bls_states_db[state_code] + '\t' + series_id + '\t' + employment + '\n')

del requests
del json
series_ids_value_textfile.close()

