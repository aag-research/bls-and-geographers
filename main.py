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
import datetime
import shutil


""" WORKSPACE """
main_folder = r'C:\Users\cdony\Google Drive\GitHub\bls-and-geographers'
#main_folder = r'C:\Users\oawowale\Documents\GitHub\bls-and-geographers'
os.chdir(main_folder)

# Create new folder for outputs based on date and time
date = datetime.datetime.now().strftime("%Y%M%d_%H%M%S")
foldername_outputs = date + '_outputs'
folder_outputs = os.path.join('script_outputs', foldername_outputs)
os.makedirs(folder_outputs)

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

# List of state codes
state_codes = list(bls_states_db.keys())

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
textfilepath = os.path.join('data', textfilename)
textfilecontent_aslist = open(textfilepath).readlines()
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
for state_code in state_codes:
  for occupation_code in aag_occupations:
    series_id = prefix + seasonal_code + area_type_code + state_code + area_code + industry_code + occupation_code + data_type
    series_ids += [series_id]

# Write the list to a textfile for documentation
filename_seriesIDs = r'BLS series ids requested.txt'
filepath_seriesIDs = os.path.join(folder_outputs, filename_seriesIDs)
file_seriesIDs = open(filepath_seriesIDs, 'w')
file_seriesIDs.write('\n'.join(series_ids))
file_seriesIDs.close()


# Determine where we will store the responses from the API
textfilename_responses = r'BLS_API_responses.txt' 
filepath_responses = os.path.join(folder_outputs, textfilename_responses)

""" SEND REQUESTS TO BLS API """

### Create an empty file in which the responses from the API will be stored
##textfile_responses = open(filepath_responses, 'w')
##textfile_responses.close()  
##
### Split the list into chucks of 50 series ids
### To maximize the number of requests based on the APIs limits
### See: https://www.bls.gov/developers/api_faqs.htm#register1
##series_ids_chunks = [series_ids[i:i+50] for i in range(0, len(series_ids), 50)]
##for series_ids_chunk in series_ids_chunks:
##  data_query = json.dumps({"seriesid": series_ids_chunk,
##                           "startyear": 2018,
##                           "endyear": 2018,
##                           "registrationkey": '41d57752042240da84a71fd2ba7c748d'})
##
##  # Send the query to the BLS API
##  post_request = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/',
##                                data = data_query,
##                                headers = {'Content-type': 'application/json'})
##
##  # Get the API response as text and convert it to a JSON format
##  responses_asjson = json.loads(post_request.text)
##
##  # Write each response to the textfile
##  textfile_responses = open(filepath_responses, 'a')
##  for response in responses_asjson['Results']['series']:
##    series_id = response['seriesID']
##    textfile_responses.write(str(response) + '\n')
##  textfile_responses.close()

""" OR: COPY BLS RESPONSES FROM A PREVIOUS RUN """
foldername_previous_run = '20194126_164146_outputs'
folderpath_previous_run = os.path.join('script_outputs', foldername_previous_run)
for filename in os.listdir(folderpath_previous_run):
    filepath = os.path.join(folderpath_previous_run, filename)
    shutil.copy(filepath, folder_outputs)

""" RE-FORMAT API RESPONSES AS NEEDED : A """

# Each row is a state and each column is a geography occupation
# the value in each cell is the total employment
spreadsheet_format = [[bls_states_db[state_code]] + ['*']*len(aag_occupations) for state_code in state_codes]
for response in open(filepath_responses).readlines():
  response = response.replace("'", '"')
  response_db = json.loads(response)
  series_id = response_db['seriesID']
  state_code = series_id[4:6]
  state_index = state_codes.index(state_code)
  occupation_code_6digit = series_id[17:23]
  occupation_index = aag_occupations.index(occupation_code_6digit) + 1
  try: employment = response_db['data'][0]['value'].replace('-', 'est. not rel.')
  except: employment = 'none'
  spreadsheet_format[state_index][occupation_index] = employment

# Write this format to a texfile
textfilename_geo_employment = r'Employment per state for Geography Occupations.txt' 
filepath_geo_employment = os.path.join(folder_outputs, textfilename_geo_employment)
filepath_geo_employment = open(filepath_geo_employment, 'w')
filepath_geo_employment.write('State\t' + '\t'.join(aag_occupations)+ '\n')
for row in spreadsheet_format:
  filepath_geo_employment.write('\t'.join(row) + '\n')
filepath_geo_employment.close()  

