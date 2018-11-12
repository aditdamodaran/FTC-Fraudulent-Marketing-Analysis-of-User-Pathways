import numpy as np
from numpy import nan as Nan
import pandas as pd
import pylab
import math
import sys
import pickle
import csv
import os

# Reads a CSV file into a Pandas dataframe
def readCSV(path, filename, columns):
    if (columns[0] == "all"):
        df = pd.read_csv(path+filename+".csv")
        return df
    else:
        df = pd.read_csv(path+filename+".csv", skipinitialspace=True, usecols=columns)
        return df


raw_data = "1_2011"
urls = readCSV("csv/input/", "fake_news_domains_extended_list", ["all"])
extended_list_of_fake_news_domains_600 = urls.ix[:,0].values.tolist()
