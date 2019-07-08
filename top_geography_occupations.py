# Author:                       Eni Awowale
# Date first written:           July 08, 2019
# Date last updated:
# Purpose:                      Calculate the Geography employment value

#Problem Statement:
#There 95 unique AAG Geography relate occupations. We want to calculate the occupations with the
#highest values for mapping in QGIS

#Importing necessary modules:
import os
import sys

folder = r'C:\Users\oawowale\Documents\GitHub\bls-and-geographers'
os.chdir(folder)

#occupational_employment_textfile_name = r'occupational_employment_values_for_reading.txt'
occupational_employment_textfile = open('occupational_employment_values_for_reading.txt').readlines()
occupational_codes =  occupational_employment_textfile[0].strip('State\t').strip('\n')
occupational_codes = occupational_codes.split('\t')

state_values_list = []
state_values_db = {}
#loop for saving textfile into dictionary for state
for line in occupational_employment_textfile[1:]:
    #print(line[0])
    state_name = line.split('\t')[0]
    state_values_db [state_name] = {}
    occupational_values = line.split('\t')
    #state_values_list.append(line[1:])
    for occupation in occupational_codes:
        state_values_db [state_name]= {occupation}
        for occupational_value in occupational_values:
            state_values_db [state_name] = {occupation: occupational_value}
#print(state_values_db)

