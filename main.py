# Author:                       Eni Awowale & Coline Dony
# Date first written:           December 18, 2018
# Date last updated:            July 268, 2019
# Purpose:                      Extract BLS data

# Problem Statement:
# For several projects at the AAG, it would be valuable to utilize data
# made available by the Bureau of Labor Statistics (BLS) on jobs
# and occupations. This main framework calls different modules that
# were developed to extract these data using the BLS API and re-format these
# data in different formats (spreadsheets, maps, etc.) as needed.

""" PYTHON LIBRARIES
    all libraries necessary to run all scripts """

import os
import requests
import json


""" WORKSPACE """
folder = r'C:\Users\cdony\Google Drive\GitHub\bls-and-geographers'
#folder = r'C:\Users\oawowale\Documents\GitHub\bls-and-geographers'
os.chdir(folder)


""" BLS DICTIONARIES
    dictionaries that will help translate BLS codes """

## State codes: read the states code dictionary from BLS website
## The BLS State codes represent each state and its name as used in their databases 
## see https://download.bls.gov/pub/time.series/sa/sa.state
url = r'https://download.bls.gov/pub/time.series/sa/sa.state'
urlcontent_astext = requests.get(url).text

# Create an state codes dictionary
bls_states_db = {}
for line in urlcontent_astext.split('\n')[1:]:
  try: state_code, state_name = line.strip().split('\t')
  except: continue
  bls_states_db[state_code] = state_name

## Occupation codes: read the occupation code dictionary from BLS website
## The BLS Occupation codes represent the 6-digit code and its name as used in their databases 
## see https://download.bls.gov/pub/time.series/oe/oe.occupation
url = r'https://download.bls.gov/pub/time.series/oe/oe.occupation'
urlcontent_astext = requests.get(url).text

# Create an occupation codes dictionary
bls_occupations_db = {}
for line in urlcontent_astext.split('\n')[1:]:
  try: occupation_code_6digit, occupation_name = line.strip().split('\t')[:2]
  except: continue
  bls_occupations_db[occupation_code_6digit] = occupation_name

""" GEOGRAPHY OCCUPATIONS
    Occupations the AAG has been tracking because they relate to geography """

## AAG salary data: read the occupation codes associated with geography jobs
## from the AAG salary spreadsheet (provided by Mark Revell in June 2019)
textfilename = r'Salary Data 2018 updated.txt'
textfilecontent_aslist = open(textfilename).readlines()
headers = textfilecontent_aslist[0].split('\t')

# Create an AAG occupations dictionary
aag_occupations_db = {}
for line in textfilecontent_aslist[1:]:
  # The occupations (occ) in the salary data file are based on the BLS 8-digit codes
  occ_name_8digit, occ_code_8digit = line.split('\t')[:2]
  if occ_code_8digit != '':
    # To make queries to the BLS API, we will need 6-digit occupation codes instead
    occ_code_6digit = occ_code_8digit.replace('-', '').replace('.', '')[:6]
    try: occ_name_6digit = bls_occupations_db[occ_code_6digit]
    except:
      # Some 6-digit codes are not listed in the occupation codes dictionary
      # For those, replacing the last digit by a zero will lead to the most
      # closely related 6-digit code
      occ_code_6digit = occ_code_6digit[:5] + '0'
      occ_name_6digit = bls_occupations_db[occ_code_6digit]

    #adding the occupational codes the agg_occupations_db
    if occ_code_6digit not in aag_occupations_db:
      aag_occupations_db[occ_code_6digit] = { 'Main occupation name': occ_name_6digit,
                                              'Geography occupations': {occ_code_8digit : occ_name_8digit}}
    else: aag_occupations_db[occ_code_6digit]['Geography occupations'][occ_code_8digit] = occ_name_8digit

# List of 6-digit occupation codes that relate to geography jobs
aag_occupations = list(aag_occupations_db.keys())


""" BLS SERIES IDS
    Making a request to the BLS API happens by sending a Series ID,
    which is made of specific characters that form a query """

# BLS format to request employment data
# See: https://www.bls.gov/help/hlpforma.htm#OE
prefix = 'OE'               # Occupational Employment
seasonal_code = 'U'         # Seasonal
area_type_code = 'S'        # Statewide
state_code = None           # 01 stands for Alabama for example
area_code = '00000'         # Statewide
industry_code = '000000'    # Cross-industry, Private, Federal, State, and Local Government
occupation_code = None      # 19-3092 stands for geogrpaher for example
data_type = '01'            # Employment

# Create a list of series ids we are interested in (following the BLS format)
series_ids = []
for state_code in bls_states_db:
  for occupation_code in aag_occupations:
    series_id = prefix + seasonal_code + area_type_code + state_code + area_code + industry_code + occupation_code + data_type
    series_ids += [series_id]

# Write the list to a textfile for documentation
filename_seriesIDs = open('BLS series ids requested.txt', 'w')
filename_seriesIDs.write('\n'.join(series_ids))
filename_seriesIDs.close()

# Run BLS requests and store data in a textfile
textfilename_BLSresponses = r'bls_state_occupational_employment.txt'

# Read responses from the text file and calculate
# the top 5 occupations
textfilecontent_BLSresponses = open(textfilename_BLSresponses).readlines()
bls_states_values_db = {}
for line in textfilecontent_BLSresponses[1:]:
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

  # Print out the top 5 in each state
  for occupation_code in bls_states_values_db[state_name]['top 5']:
    occupation_name = aag_occupations_db[occupation_code]['Main occupation name']
    employment = bls_states_values_db[state_name][occupation_code]['employment text']
    print(occupation_name, ' : ', employment)


