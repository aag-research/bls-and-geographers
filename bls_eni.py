# Author:                       Eni Awowale & Coline Dony
# Date first written:           December 18, 2018
# Date last updated:            July 08, 2019
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
import operator

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
bls_states_values_db = {}

#Creating state dictionary
for line in BLS_dictionary_textfile.split('\n')[1:]:
  try: state_code, state_name = line.strip().split('\t')
  except: continue
  bls_states_db[state_code] = state_name


#Same thing as url request above but instead it is for occupations
BLS_dictionary_url = r'https://download.bls.gov/pub/time.series/oe/oe.occupation'
BLS_dictionary_textfile = requests.get(BLS_dictionary_url).text
bls_occupations_db = {}

for line in BLS_dictionary_textfile.split('\n')[1:]:
  try: occupation_code_6digit, occupation_name = line.strip().split('\t')[:2]
  except: continue
  bls_occupations_db[occupation_code_6digit] = occupation_name

AAG_salary_data_textfilename = r'Salary Data 2018 updated.txt'
AAG_salary_data_textfile = open(AAG_salary_data_textfilename).readlines()
headers = AAG_salary_data_textfile[0].split('\t')
aag_occupations_db = {}


for line in AAG_salary_data_textfile[1:]:
  # The occupations in the salary data file are based on the BLS 8-digit codes
  occ_name_8digit, occ_code_8digit = line.split('\t')[:2]
  #splits by tab
  if occ_code_8digit != '':
    # To make queries to the BLS API, we need the 6-digit code instead
    occ_code_6digit = occ_code_8digit.replace('-', '').replace('.', '')[:6]
    try: occ_name_6digit = bls_occupations_db[occ_code_6digit]
    #adding a zero to the end of the 6 digit codes so it is the correct length
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
bls_api_key_Eni_3 = '8649eca48fd747478b7ba9a7095b2473'
#bls_api_key_Eni_2 = '9954b4145b514eae81c6fe9a93739299'
#bls_api_key_Eni = 'de6366639eb64fa79045c9071a080dd5'
#bls_api_key_Coline = '41d57752042240da84a71fd2ba7c748d'
#bls_api_key = '41d57752042240da84a71fd2ba7c748d'


#Creating new text file in a suitable format for joining attribute tables TIGER Line Shapefiles
'''
state_occupational_employment_textfile = open('state_occupational_employment.txt', 'w')
state_occupational_employment_textfile.write('State\t' + '\t'.join(aag_occupations))
state_values = []
state_values_appending = []
'''
#Part 1: Requesting Occupational Data
'''
for series_ids_chunk in series_ids_chunks:
  data_query = json.dumps({"seriesid": series_ids_chunk,
                         "startyear": startyear,
                         "endyear": endyear,
                         "registrationkey": bls_api_key_Eni_3})
  # BLS API location
  bls_api_location = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'

  # Query Header: requests the response to be in the JSON format
  headers = {'Content-type': 'application/json'}

  # Send the query to the BLS API
  post_request = requests.post(bls_api_location, data=data_query, headers=headers)

  # Get the API response as text and convert it to a JSON dictionary
  get_response = json.loads(post_request.text)

  # Extracting data from the API response
  for series in get_response['Results']['series']:
    series_id = series['seriesID']
    state_code = series_id[4:6]
    state_name = bls_states_db[state_code]
    occupation_code_6digit = series_id[17:23]
    occupation_index = aag_occupations.index(occupation_code_6digit)
    try:
      employment_text = series['data'][0]['value'].replace('-', 'no est.')
      try:
        employment_int = int(employment_text)
      except ValueError:
        employment_int = 0
    except:
      employment_int = 0
      employment_text = 'none'
    if state_name not in bls_states_values_db:
      bls_states_values_db[state_name] = {}
    if employment_text and occupation_code_6digit not in bls_states_values_db:
      bls_states_values_db[state_name][occupation_code_6digit] = employment_int
        #adding state names to new dictionary with values


    if state_name not in state_values:
      state_occupational_employment_textfile.write('\t'.join(state_values) + '\n')
      state_values = [state_name] + ['*']*len(aag_occupations)
    state_values[occupation_index + 1] = employment_text
state_occupational_employment_textfile.write('\t'.join(state_values) + '\n')
state_occupational_employment_textfile.close()
'''
#Part 2: Comparing Occupational Data
state_employment_textfile_reading = open('occupational_employment_values_for_reading.txt').readlines()
occupation_codes_text = state_employment_textfile_reading[0].strip('State\t')
# for state in bls_states_db.values():
#   bls_states_values_db[state]={}
for state_data in state_employment_textfile_reading:
  occupation_codes = occupation_codes_text.split('\t')
  state_names = state_data.split('\t')[0]
  if state_names != 'State':
    clean_state_name = state_names
    bls_states_values_db[clean_state_name]={}
    for code in occupation_codes:
      #bls_states_values_db[clean_state_name][code]={}
      for employment_value in range(len(state_employment_textfile_reading)):
        values = state_employment_textfile_reading[employment_value].split('\t')
        bls_states_values_db[clean_state_name][code] = values[employment_value]
        print(values[1])


#bls_states_values_db[state_names][employment_values]={}
# for employment_value in state_employment_textfile_reading[1:]:
  #   state_values=employment_value.split('\t')
  #   bls_states_values_db[state_values[0]][occupation_code] = {}

# for state in bls_states_db.values():
#   bls_states_values_db[state]={}
#   for occupation_code in headers_employment_textfile[1:]:
#     bls_states_values_db[state][occupation_code]='value'

#state_occupational_values_list = [[] for occ_value in range(len(bls_states_values_db))]
state_occupational_value={}
#state_occupation_values = bls_states_values_db
#Changing values to integer so they can be compared:
# state_occupational_values_list=[]
# for occupation, employment_data in bls_states_values_db.items():
#     for occupation_code, occupation_value in employment_data.items():
#         state_occupational_values_list.append((occupation_value, occupation_code))
# top_5=sorted(state_occupational_values_list,reverse=True)[:5]





'''
for value in bls_states_values_db.values():
    for key, occupation_value in value.items():
        if occupation_value.isdigit():
            occupation_value = int(occupation_value)
            new_occupation_value = int(occupation_value)
            value[key] = occupation_value
            state_occupational_values_list2.append(new_occupation_value)
            for list in range(0,95):
              state_occupational_values_list[0:].append(state_occupational_values_list2[0:-1:95])
        else:
          value[key]=0
          state_occupational_values_list2.append(0)
          for list in range(len(state_occupational_values_list)):
            state_occupational_values_list[0].append(state_occupational_values_list2[0:-1:95])
        # state_occupational_values_list = [[] for occ_value in range(len(bls_states_values_db))]'''
        # state_occupational_values_list.append(new_occupation_value)

    #for num in range(0,95):
      #state_occupation_values = dict(sorted(value.items(), key=operator.itemgetter(num), reverse=True)[:5])



del requests
del json

