import numpy as np
from numpy import nan as Nan
import pandas as pd
import pylab
import math
import sys
import pickle
import csv
import os
from globals import *

# Takes a text file list and converts to a Python list
def convertListToPyList(filename):
    list_file = open(filename+".txt", 'r')
    list_text = list_file.read()
    list = list_text.splitlines()
    return list

# Read Compressed Data from PKL Filetype in PKL Subdirectory
# Assign to Global Datafrane Variable "data"
def readRawDataPKL(filename):
    global data
    data = pd.read_pickle("pkl/raw_data/"+filename+".pkl.compress", compression="gzip")
    return data


# Read Compressed Data from PKL Filetype in PKL Subdirectory
# Assign to Global Datafrane Variable "data"
def readDataPKL(path, filename):
    global data
    data = pd.read_pickle(path+filename+".pkl.compress", compression="gzip")
    return data

# Reads a CSV file into a Pandas dataframe
def readCSV(path, filename, columns):
    if (columns[0] == "all"):
        df = pd.read_csv(path+filename+".csv")
        return df
    else:
        df = pd.read_csv(path+filename+".csv", skipinitialspace=True, usecols=columns)
        return df

def readCSV_NoHeaders(path, filename, columns):
    if (columns[0] == "all"):
        df = pd.read_csv(path+filename+".csv", header=None)
        return df
    else:
        df = pd.read_csv(path+filename+".csv", skipinitialspace=True, header=None, usecols=columns)
        return df


# Gets relevant rows of a dataframe based on a filter list input
def getRelevantRows(dataframe, columns, filter_list):
    relevant_rows = pd.DataFrame()
    for column in columns:
        relevant_row = dataframe.loc[dataframe[column].isin(filter_list)]
        relevant_rows = relevant_rows.append(relevant_row)
    return relevant_rows

# Take Data and Split it Into Separate CSVs by
# User ID (machine_id)
def splitByUserID(dataframe, filter_list):
    root = "user_csvs/"
    users = dataframe['machine_id'].unique()
    spot_relevant_data_compiled = pd.DataFrame()
    for user in users:
        output_filename = str(user) + '.csv'
        data = dataframe.loc[dataframe['machine_id'] == user]
        spot_relevant_data = data.loc[data['ref_domain_name'].isin(filter_list)]
        spot_relevant_data_compiled = spot_relevant_data_compiled.append(spot_relevant_data)
        spot_relevant_data = data.loc[data['domain_name'].isin(filter_list)]
        spot_relevant_data_compiled = spot_relevant_data_compiled.append(spot_relevant_data)

        print(output_filename)
        data.to_csv(os.path.join(root, output_filename), encoding='utf-8')

    spot_relevant_data_compiled.to_csv("csv/UserIDrelevantspots.csv", encoding='utf-8')

# Gets Duped Machine IDs
def getDupedMachines(data_of_machines, alldata):
    machine_ids_duped = data_of_machines['machine_id'].tolist()
    all_sites_visited_by_duped_machines = alldata.loc[alldata['machine_id'].isin(machine_ids_duped)]
    return all_sites_visited_by_duped_machines

# Export CSV File Wrapper Function
def exportAsCSV(filename, path, data):
    data.to_csv(path+filename+".csv", encoding='utf-8')

# Export PKL File Wrapper Function
def exportAsPKL(filename, path, data):
    data.to_pickle(path+filename+".pkl.compress", compression="gzip")

# Convert row with timestamp in form %HH:%MM:%SS to string
def getTimestampAsString(row):
    return str(row["event_time"])

def getTimestampItemAsString(row):
    return str(row["event_time"].item())

# Check that a time_0 is within an interval before time_f
def withinInterval(time_0, time_f, interval):
    time_delta = str(datetime.strptime(time_f,"%H:%M:%S") - datetime.strptime(time_0,"%H:%M:%S"))
    time_delta = time_delta.split(":")
    if (int(time_delta[1]) >= interval):
        return 0
        # Not Within Interval
    else:
        return 1
        # Within Interval

def printSummary(row):
    print(row.filter(items=["machine_id", "event_time", "event_date", "ref_domain_name", "domain_name"]))

# export a Python List as a CSV
def exportListAsCSV(list, path, filename):
    rows = zip(list)
    with open(path+filename+".csv", "w") as output_file:
        writer = csv.writer(output_file)
        for row in rows:
            writer.writerow(row)

def exportCSVandPKL(name, data):
    exportAsCSV(name+"_"+raw_data,"csv/output/"+raw_data+"/",data)
    if not os.path.exists("pkl/"+name+"/"+raw_data):
        os.makedirs("pkl/"+name+"/"+raw_data)
    exportAsPKL(name+"_"+raw_data,"pkl/"+name+"/"+raw_data+"/",data)
