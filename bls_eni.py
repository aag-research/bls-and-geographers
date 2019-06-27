# Author:                       Eni Awowale & Coline Dony
# Date first written:           December 18, 2018
# Date last updated:            June 27, 2019
# Purpose:                      Extract BLS data

# Problem Statement:
# For several projects at the AAG, it would be valuable to utilize data
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

# Coline's and Eni's Directories
#folder = r'C:\Users\cdony\Google Drive\GitHub\bls-and-geographers'
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

#Url request so we can get the states and state codes
BLS_dictionary_url = r'https://download.bls.gov/pub/time.series/sa/sa.state'
BLS_dictionary_textfile = requests.get(BLS_dictionary_url).text
bls_states_db = {}

for line in BLS_dictionary_textfile.split('\n')[1:]:
  #Skip the first line loop and split to list by  each next line
  try: state_code, state_name = line.strip().split('\t')
  #initializing variables state code and state name
  except: continue
  bls_states_db[state_code] = state_name
  #adding items to bls_states_db: keys are state codes, values are state names

# A2. Occupation codes: read occupation codes that relate to Geography occupation
# The AAG collects salaries from the BLS website for geography-related occupations
# We use the AAG list of occupation

#Same thing as url request above but instead it is for occupations
BLS_dictionary_url = r'https://download.bls.gov/pub/time.series/oe/oe.occupation'
BLS_dictionary_textfile = requests.get(BLS_dictionary_url).text
bls_occupations_db = {}

for line in BLS_dictionary_textfile.split('\n')[1:]:
  try: occupation_code_6digit, occupation_name = line.strip().split('\t')[:2]
  #splits and strips at tab, saves both variables
  except: continue
  bls_occupations_db[occupation_code_6digit] = occupation_name

AAG_salary_data_textfilename = r'Salary Data 2018 updated.txt'
AAG_salary_data_textfile = open(AAG_salary_data_textfilename).readlines()
headers = AAG_salary_data_textfile[0].split('\t')
aag_occupations_db = {}


for line in AAG_salary_data_textfile[1:]:
  #Skips headers on textfile
  # The occupations in the salary data file are based on the BLS 8-digit codes
  occ_name_8digit, occ_code_8digit = line.split('\t')[:2]
  #splits by tab
  if occ_code_8digit != '':
    # To make queries to the BLS API, we need the 6-digit code instead
    occ_code_6digit = occ_code_8digit.replace('-', '').replace('.', '')[:6]
    try: occ_name_6digit = bls_occupations_db[occ_code_6digit]
    except:
      occ_code_6digit = occ_code_6digit[:5] + '0'
      occ_name_6digit = bls_occupations_db[occ_code_6digit]

    #adding the occupational codes the agg_occupations_db
    if occ_code_6digit not in aag_occupations_db:
      aag_occupations_db[occ_code_6digit] = { 'Main occupation name': occ_name_6digit,
                                              'Geography occupations': {occ_code_8digit : occ_name_8digit}}
    else: aag_occupations_db[occ_code_6digit]['Geography occupations'][occ_code_8digit] = occ_name_8digit


#Creating a list and text file of multiple series_ids
series_id_list = []
series_ids_file = open('list_series_id.txt', 'w')
#Initializing list AGG of occupations
aag_occupations = list(aag_occupations_db.keys())

#Initializing variable series_id that follows the BLS series_id format
for state_code in bls_states_db:
  for occupation_code in aag_occupations:
    series_id = prefix + seasonal_code + area_type_code + state_code + area_code + industry_code + occupation_code + data_type
    series_id_list.append(series_id)
    series_ids_file.write(series_id + '\n')
series_ids_file.close()

# A4. Split the list into chucks of 50 series ids
series_ids = [series_id.strip() for series_id in open('list_series_id.txt').readlines()]
series_ids_chunks = [series_ids[i:i+50] for i in range(0, len(series_ids), 50)]

# B. The years we want to extract from
startyear = 2018
endyear = 2018

# C. Make Requests to the BLS API, using multiple series ids

# BLS API keys:
bls_api_key_Eni = 'de6366639eb64fa79045c9071a080dd5'
#bls_api_key_Coline = '41d57752042240da84a71fd2ba7c748d'

# BLS API query
data_query = json.dumps({"seriesid": series_ids_chunks[0],
                         "startyear":startyear,
                         "endyear":endyear,
                         "registrationkey": bls_api_key_Eni})

# BLS API location
bls_api_location = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'

# Query Header: requests the response to be in the JSON format
headers = {'Content-type': 'application/json'}

# Send the query to the BLS API
post_request = requests.post(bls_api_location, data=data_query, headers=headers)

# Get the API response as text and convert it to a JSON dictionary
get_response = json.loads(post_request.text)

#Creating new text file in a suitable format for joining attribute tables TIGER Line Shapefiles
state_occupational_employment_textfile = open('state_occupational_employment.txt', 'w')
state_occupational_employment_textfile.write('State\t' + '\t'.join(aag_occupations))

#Starting Request
state_values = []
for series in get_response['Results']['series']:
  series_id = series['seriesID']
  state_code = series_id[4:6]
  state_name = bls_states_db[state_code]
  occupation_code_6digit = series_id[17:23]
  occupation_index = aag_occupations.index(occupation_code_6digit)
  try: employment = series['data'][0]['value'].replace('-', 'no est.')
  except: employment = 'none'
  if state_name not in state_values:
    state_occupational_employment_textfile.write('\t'.join(state_values) + '\n')
    state_values = [state_name] + ['*']*len(aag_occupations)
  state_values[occupation_index + 1] = employment
state_occupational_employment_textfile.write('\t'.join(state_values) + '\n')

del requests
del json
state_occupational_employment_textfile.close()
