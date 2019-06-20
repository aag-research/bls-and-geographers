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

import requests
import json

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


# Per the format  for requesting data
prefix = 'OE'               #Occupational Employment
seasonal_code = 'U'         # Seasonal
area_type_code = 'S'        # Statewide
state_code = ''             #empty string will use in for loop
state_range = range(1,79)   #possible ranges of states and us territories
area_code = '00000'         # Statewide
industry_code = '000000'    # Cross-industry, Private, Federal, State, and Local Government
#list of occupational codes based on AAG Salary Data
list_occu_codes = [251041,251061,251031,251062,251051,252021,251032,251053,251043,193092,251064,251191,193093,251125,252022,251065,193094,252031,251067,259041,194011,172021,532021,532022,171011,173011,171021,173022,172051,173025,172081,194091,194093,194041,171012,172121,172151,173031,171022,273042,536041,193051,273043,193091,131161,193041,193022,397011,413041,397012,192021,191031,192041,454011,191032,192042,192043,113071,119121,119141,119161,132021,172171,191013,191023,193011,251063,254012,332022,333031,419021,419022,452011,194041,151199,151199,171022,192099,173011,173031,194099,173031,194061,333021,192041,194099,119121,193091,193099,113071,192041,435011,434181,112021,112022,113021,131081,131121,151111,151121,151131,151132,151133,151151,152031,191012,251061,254011,435071]
occupation_code = ''        #empty string will use in for loop for occupations
data_type = '01'            # Employment

#file for all of the series ids
series_id_file = open('list_series_id.txt', 'w') #created a file to store all of the series_ids
series_id_list = []
#for loop for creating different state codes
for code in state_range:
    #exceptions for values in range that aren't state codes
    if code in [3,7,14,43,52]:
        continue
    #exception for U.S. territories
    elif code > 56 and code not in [66,72,78]:
        continue
    else:
        code_str=str(code)  #code str is the value of code
        state_code = code_str.zfill(2) #use zfill to fill out the single digit numbers
    #loop for occupational codes
    for occupation in list_occu_codes:
        occupation_code = str(occupation)
# All the codes are then combined within the for loop to create a series_id
        series_id = prefix + seasonal_code + area_type_code + state_code + area_code + industry_code + occupation_code + data_type
        #writing all of the series ids into a textfile
        series_id_file.write(series_id + '\n')
series_id_file.close()
#stopped here now i just want it to read the first 50 responses as a list later in the data_query

#openning the list of series id file
textFile_series_id = open('list_series_id.txt', 'r')
#reading the ids
for id in textFile_series_id.readlines():
    #strip to format the ids
    id_strip = id.strip('\n')
    #conditional statement for adding ids into a list
    if id_strip in series_id_list:
        continue
    elif id_strip is not series_id_list:
        #saving id into list
        series_id_list.append(id_strip)
#print(series_id_list[0:50])

# B. (Years) What years do we want to extract data from?
startyear = 2018
endyear = 2018

# C1. One Query with an API key
#Eni's bls api key
bls_api_key = 'de6366639eb64fa79045c9071a080dd5'
#Coline's bls api key
#Col_bls_api_key = '41d57752042240da84a71fd2ba7c748d'
#So it is more with this part

data_query = json.dumps({"seriesid": series_id_list[0:], "startyear":startyear, "endyear":endyear, "registrationkey": bls_api_key})
#print(data_query)
#D. BLS API location
bls_api_location = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'

# E. Data query (see 3C. above)

# F. Header
# Request the response to be in the JSON format
headers = {'Content-type': 'application/json'}

#Send the query to the BLS API
post_request = requests.post(bls_api_location, data=data_query, headers=headers)
#
# ## G. Get the API response as text and convert it to a JSON dictionary
get_response = json.loads(post_request.text)

#to get the value of the state
#print(get_response['Results']['series'][1])

textFile_series_id.close()

file_series_id_value = open('series_id_value.txt', 'w')
#I decided to make a new textfile with both the values and series_ids
#This for loop only looks at the range of 3 series_ids
#You can get it to look at more my setting the range to look at a certain amount of values
#My plan is to just change the ranges for each query I make so it keeps appending to the textfile
#So when I start doing the query I am going to change file_series_id_value to append mode 'a'
#I have already tried it this way and it works
for val in range(0,3):
    #creating variables for the states_series_ids and state_values
    state_series_id = get_response['Results']['series'][val]['seriesID']
    state_values = get_response['Results']['series'][val] ['data'] [0] ['value']
    #adding the values to my new textfile
    file_series_id_value.write(state_series_id + '\t' + state_values + '\n')
del requests
del json
file_series_id_value.close()


#Something to think about later for states that don't have values
#some states don't have values, so we want to the script keeps running despite that
#fileAppend_series_id = open('list_series_id.txt', 'a+')
#so if there is a series value == true set employ_val equal to that value
# for value in get_response['Results']['series'] [0]:
#     #employ_val = get_response['Results']['series'][0:]
#     if value != 'value':
#         employ_val = get_response['Results']['series'][0:]
#         print(type(employ_val))
#then save this value to text file
    #fileAppend_series_id.write(employ_val)
