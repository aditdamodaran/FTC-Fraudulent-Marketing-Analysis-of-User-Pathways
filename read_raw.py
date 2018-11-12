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

# Reads Raw Data CSV file into a Pandas dataframe
def readRawCSV(path, type, filename, columns):
    if (columns[0] == "all"):
        df = pd.read_csv("csv/"+type+"/"+path+filename+".csv", encoding = "ISO-8859-1")
        return df
    else:
        df = pd.read_csv(path+filename+".csv", skipinitialspace=True, usecols=columns)
        return df

# Export Raw Data as PKL File
def exportRawAsPKL(filename, data):
    data.to_pickle("pkl/raw_data/"+filename+".pkl.compress", compression="gzip")

# MAIN FUNCTION
def main():
    data = readRawCSV("raw_data_month/","input",raw_data,["all"])
    exportRawAsPKL(raw_data, data)

main()
