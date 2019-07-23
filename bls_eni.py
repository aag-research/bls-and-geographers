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

# Url request so we can get the states and state codes
BLS_dictionary_url = r'https://download.bls.gov/pub/time.series/sa/sa.state'
BLS_dictionary_textfile = requests.get(BLS_dictionary_url).text
bls_states_db = {}

# Creating state dictionary
for line in BLS_dictionary_textfile.split('\n')[1:]:
  try: state_code, state_name = line.strip().split('\t')
  except: continue
  bls_states_db[state_code] = state_name


# A2. Occupation codes: read the occupation code dictionary from BLS website
# The BLS Occupation codes represent the 6-digit code and its name as used in their databases 
# see https://download.bls.gov/pub/time.series/oe/oe.occupation
BLS_dictionary_url = r'https://download.bls.gov/pub/time.series/oe/oe.occupation'
BLS_dictionary_textfile = requests.get(BLS_dictionary_url).text
bls_occupations_db = {}

for line in BLS_dictionary_textfile.split('\n')[1:]:
  try: occupation_code_6digit, occupation_name = line.strip().split('\t')[:2]
  except: continue
  bls_occupations_db[occupation_code_6digit] = occupation_name

# A3. AAG salary data: read the occupation codes associated with geography jobs
# from the AAG salary spreadsheet (provided by Mark Revell in June 2019)
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

# List of 6-digit occupation codes that relate to geography jobs
aag_occupations = list(aag_occupations_db.keys())

# A4: Series_ids: Create a list of the series ids we are interested
# to request to the BLS API (and store them to a text file for
# documentation purposes)
series_ids = []
series_ids_file = open('list_series_id.txt', 'w')

#Initializing variable series_id that follows the BLS series_id format
for state_code in bls_states_db:
  for occupation_code in aag_occupations:
    series_id = prefix + seasonal_code + area_type_code + state_code + area_code + industry_code + occupation_code + data_type
    series_ids += [series_id]
    series_ids_file.write(series_id + '\n')
series_ids_file.close()

# Split the list into chucks of 50 series ids
series_ids_chunks = [series_ids[i:i+50] for i in range(0, len(series_ids), 50)]

# B. The years we want to extract from
startyear = 2018
endyear = 2018

# C. Make Requests to the BLS API (using multiple series ids)
# https://www.bls.gov/developers/api_signature_v2.htm#multiple

# BLS API keys:
#bls_api_key_Eni_3 = '8649eca48fd747478b7ba9a7095b2473'
#bls_api_key_Eni_2 = '9954b4145b514eae81c6fe9a93739299'
#bls_api_key_Eni = 'de6366639eb64fa79045c9071a080dd5'
bls_api_key_Coline = '41d57752042240da84a71fd2ba7c748d'
#bls_api_key = '41d57752042240da84a71fd2ba7c748d'

# Store bls responses to a text file
bls_responses_textfilename = r'bls_state_occupational_employment.txt'
'''
bls_responses_textfile = open(bls_responses_textfilename, 'w')
bls_responses_textfile.write('State\t' + '\t'.join(aag_occupations))

state_values = []
for series_ids_chunk in series_ids_chunks:
  data_query = json.dumps({"seriesid": series_ids_chunk,
                         "startyear": startyear,
                         "endyear": endyear,
                         "registrationkey": bls_api_key_Coline})
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
    try: employment = series['data'][0]['value'].replace('-', 'no est.')
    except: employment = 'none'
    if state_name not in state_values:
      bls_responses_textfile.write('\t'.join(state_values) + '\n')
      state_values = [state_name] + ['*']*len(aag_occupations)
    state_values[occupation_index + 1] = employment
bls_responses_textfile.write('\t'.join(state_values) + '\n')
bls_responses_textfile.close()
'''
top_five_occupations_textfile = open(r'top_5_state_occupations.txt','w')
top_five_occupations_textfile.write('State\t' + '1st\t' + '2nd\t' + '3rd\t' + '4th\t' + '5th')
top_values = []

# Read responses from the text file and calculate
# the top 5 occupations
bls_responses_textfile = open(bls_responses_textfilename).readlines()
bls_states_values_db = {}
for line in bls_responses_textfile[1:]:
  state_data = line.split('\t')
  state_name = state_data[0]
  bls_states_values_db[state_name] = {}

  # Convert text values into integers
  employment_ints = []
  for occupation_code, employment_text in zip(aag_occupations, state_data[1:]):
    try: employment_int = int(employment_text)
    except: employment_int = 0
    employment_ints += [employment_int]
    bls_states_values_db[state_name][occupation_code] = {'employment text': employment_text,
                                                        'employment int': employment_int}

  # Calculate top 5 occupations per state
  bls_states_values_db[state_name]['top 5'] = []
  for employment_int, occupation_code in sorted(zip(employment_ints, aag_occupations), reverse=True)[:5]:
    bls_states_values_db[state_name]['top 5'] += [occupation_code]
    top_val = bls_states_values_db[state_name]['top 5']
  if state_name and top_val not in top_values:
    top_values.append((state_name,top_val))
for state, top_occupations in top_values:
  #top_state = [top_5 for top_5 in top_occupations]
  top_five_occupations_textfile.write('\n'+ state + '\t')
  for top_5 in top_occupations:
    top_five_occupations_textfile.write(top_5 + '\t')
    #   for occupations in top_occupations:
    #     top_five_occupations_textfile.write('\t'+ occupations)
    #   top_five_occupations_textfile.write( + '\n')
    # top_five_occupations_textfile.write('\n' + employment)

# Print out the top 5 in each state
for state_name in bls_states_values_db:
  #print('\n\n')
  #print(state_name)
  for occupation_code in bls_states_values_db[state_name]['top 5']:
    occupation_name = aag_occupations_db[occupation_code]['Main occupation name']
    employment = bls_states_values_db[state_name][occupation_code]['employment text']
    #print(occupation_name, ' : ', employment)
top_five_occupations_textfile.close()
  
del requests
del json

