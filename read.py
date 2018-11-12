import numpy as np
from numpy import nan as Nan
import pandas as pd
import bokeh
import pylab
import math
import sys
import pickle
import csv
import os
from datetime import datetime, time as datetime_time, timedelta
pd.options.mode.chained_assignment = None  # default='warn'


fake_news_domains = ["online6health.com","onlinenews6.com","online6reports.com","online8report.com","memphisgazette.net",
                     "channel2local.com","channel9healthbeat.com","channel9investigates.com","consumerproductsdaily.com","nbssnewsat6.com","news4daily.tv",
                     "usahealthnewstoday.org","usahealthreportstoday.org",
                     "breakingnewsat6.com","channel9newsreport.com",
                     "consumer6report.com","consumershealth6-report.com",
                     "new6reports.com","consumerhealtwarning.com","consumerhealthwarning.com",
                     "consumernewspick.com","theconsumerweeklydigest.com","kristingotskinny.com","rachaels-blog.com",
                     "health8news.com","health8news.net","consumerdigestweekly.com",
                     "health9news.com","acai-berrytrial-offers.com","acai-trial-offers.com","www.colon-cleanse-trial-offers.com",
                     "channel6reports.com","healthnews10.com","consumertipsdaily6.com",
                     "channe18health.com","dailyhealth6.com","online6health.com",
                     "nbcreports.com","usahealthnewstoday.org","usahealthreportstoday.org",
                     "channel5healthnews.com","dailyconsumeralerts.com","online6health.com",
                     "dailyconsumeralert.org","healthylivingreviewed.com","econsumerlifestyle.com","healthydietingreports.org",
                     "diet.com-wb4.net","diet.com8s9.net","healthconsumerreviews.com","healthlifestylereview.com"]

# Takes a text file list and converts to a Python list
def convertListToPyList(filename):
    list_file = open(filename+".txt", 'r')
    list_text = list_file.read()
    list = list_text.splitlines()
    return list

# Read Compressed Data from PKL Filetype in PKL Subdirectory
# Assign to Global Datafrane Variable "data"
def readDataPKL(filename):
    global data
    data = pd.read_pickle("pkl/"+filename+".pkl.compress", compression="gzip")
    return data

# Reads a CSV file into a Pandas dataframe
def readCSV(path, filename, columns):
    if (columns[0] == "all"):
        df = pd.read_csv(path+filename+".csv")
        return df
    else:
        df = pd.read_csv(path+filename+".csv", skipinitialspace=True, usecols=columns)
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
def exportAsCSV(filename, data):
    data.to_csv("csv/"+filename+".csv", encoding='utf-8')

# Export PKL File Wrapper Function
def exportAsPKL(filename, data):
    data.to_pickle("pkl/"+filename+".pkl.compress", compression="gzip")

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
def exportListAsCSV(list, filename):
    rows = zip(list)
    with open("csv/"+filename+".csv", "w") as output_file:
        writer = csv.writer(output_file)
        for row in rows:
            writer.writerow(row)


# MAIN FUNCTION
def main():
    # List of 600+ domains purchased in court cases
    fake_news_domains_extended = convertListToPyList("fake_domains_list")

    # Data of rows (out of all the data) where ~25 unique product domains occur
    data_of_relevant_rows = readCSV("csv/","UserIDrelevantspots",["all"])

    #duped machine data
    data_of_duped_machines = readCSV("csv/","duped_machines_browsing_history_using_600",["all"])
    machine_ids_of_duped_machines = sorted(set(data_of_duped_machines["machine_id"].tolist()))

    # counters for fake_news_referrals and non_fake_news_referrals (Direct Search or Regular Ads)
    count_of_non_fake_news_referrals = 0
    count_of_fake_news_referrals = 0
    fake_news_urls_uncovered_list = []
    new_fake_news_domains = []

    # compiled prior browsing data in single dataframe for export
    compiled_pbd = pd.DataFrame()

    for machine_id in machine_ids_of_duped_machines:
         rows = data_of_duped_machines.loc[data_of_duped_machines["machine_id"] == machine_id]

         # ----------- Sort Browsing History by Date and Time Respectively
         rows['Date'] = pd.to_datetime(rows['event_date'])
         rows['Time'] = pd.to_datetime(rows['event_time'])
         rows = rows.sort_values(['Date','Time'])

         # ---- print(rows[['machine_id', 'event_time','event_date']])

         # first find the common values
         common_rows = pd.merge(data_of_relevant_rows, rows, on=["machine_id","event_time","event_date"])
         relevant_dates_list = []
         for index, row in common_rows.iterrows():
            relevant_dates_list.append(str(row["event_date"]))
         relevant_dates_list = sorted(set(relevant_dates_list))

         for date in relevant_dates_list:
             # objective find each relevant_row corresponding with that date
             # check interval


             relevant_rows_by_date = rows[(rows.event_date == date)]
             # relevant rows for each relevant date for each machine_id

             # relevant timestamped event rows for each relevant date
             relevant_timestamps_list = pd.merge(common_rows, relevant_rows_by_date, on=["event_date", "event_time"])


             for index, row in relevant_timestamps_list.iterrows():
                 # timestamp as String of each relevant event
                 event_timestamp = row["event_date"]
                 event_timestamp_string = getTimestampAsString(row)

                 # what the user was doing before they arrived at the flagged website on the same day
                 prior_browsing_data = relevant_rows_by_date.loc[(relevant_rows_by_date["event_time"]<=event_timestamp_string)]
                 # add "= to the above after '<' to include the product domain itself"

                 # clean data to specified interval
                 # browsing history within a time interval specified above prior to arriving at a product domain
                 for index, row in prior_browsing_data.iterrows():
                     time_0 = getTimestampAsString(row)
                     time_f = event_timestamp_string
                     interval = 20 # in minutes
                     if (withinInterval(time_0, time_f, interval)):
                         continue
                     else: # drop the data *after* the user arrived at the product domain
                         prior_browsing_data = prior_browsing_data.drop([index])


                 # Compiled Browsing Data
                 compiled_pbd = compiled_pbd.append(prior_browsing_data)
                 # print(compiled_pbd)

                 # for index, row in prior_browsing_data.iterrows():
                 #     pbd_domain_name = prior_browsing_data["domain_name"]
                 #     pbd_ref_domain_name = prior_browsing_data["ref_domain_name"]


                 # START ------------------------------------------------------------------------------
                 # time_0s = []
                 #
                 # pbd_0 = prior_browsing_data.loc[prior_browsing_data["domain_name"].isin(fake_news_domains_extended)]

                 # ------------------------------------------------------------------------------
                 # if pbd_0.empty:
                 #     continue
                 # else:
                 #     for index, row in pbd_0.iterrows():
                 #         print(str(row["machine_id"]) + " " + row["event_time"] + " " + row["domain_name"] + " " + row["event_date"])
                         # time_0s.append(row["event_time"])
                 #
                 # if pbd_0_ref.empty:
                 #     continue
                 # else:
                 #     for index, row in pbd_0_ref.iterrows():
                 #         time_0s.append(row["event_time"])
                 #
                 # for time_0 in time_0s:
                 #     print(time_0)



                     # print(initial_fake_news_time + "\n")
                     # delta_time = time_delta = str(datetime.strptime(time_f,"%H:%M:%S") - datetime.strptime(initial_fake_news_time, "%H:%M:%S"))
                     # print(delta_time)

                 # END ------------------------------------------------------------------------------


                 # RELEVANT SECTION
                 # pbd_0 = prior_browsing_data.loc[prior_browsing_data["domain_name"].isin(fake_news_domains_extended)]
                 # pbd_0_ref = prior_browsing_data.loc[prior_browsing_data["ref_domain_name"].isin(fake_news_domains_extended)]
                 #
                 # if pbd_0.empty:
                 #     continue
                 # else:
                 #     print(pbd_0)
                 #
                 # if pbd_0_ref.empty:
                 #     continue
                 # else:
                 #     print(pbd_0_ref)



                     # print(pbd_domain_name)
                     # print(pbd_ref_domain_name)
                     # if pbd_domain_name.any() in fake_news_domains_extended:
                     #     print("domain_name " + str(row["domain_name"]))
                     # elif pbd_ref_domain_name.any() in fake_news_domains_extended:
                     #     print("ref_domain_name " + str(row["ref_domain_name"]))
                     # else:
                     #     continue











                 # pbd_domain_name = prior_browsing_data["domain_name"].tolist()
                 # pbd_ref_domain_name = prior_browsing_data["ref_domain_name"].tolist()
                 #
                 # pbd_combined = pbd_domain_name + pbd_ref_domain_name
                 #
                 # for idx, url in enumerate(pbd_combined):
                 #     if url in fake_news_domains_extended:
                 #         fake_news_urls_uncovered_list.append(url)
                 #         print(url)
                 #         count_of_fake_news_referrals+=1
                 #     else:
                 #         if (idx == len(pbd_combined) - 1):
                 #            count_of_non_fake_news_referrals+=1
                 #         else:
                 #            if "news" in str(url):
                 #                new_fake_news_domains.append(url)
                 #            elif "report" in str(url):
                 #                new_fake_news_domains.append(url)
                 #            else:
                 #                continue


                 # if any(url in pbd_domain_name for url in fake_news_domains_extended):
                 #    count_of_fake_news_referrals+=1
                 # elif any(url in pbd_ref_domain_name for url in fake_news_domains_extended):
                 #    count_of_fake_news_referrals+=1
                 # else:
                 #    count_of_non_fake_news_referrals+=1

    # Compiled Browsing Data
    compiled_pbd = compiled_pbd.drop_duplicates()
    exportAsCSV("prior_browsing_data", compiled_pbd)

    # new_fake_news_domains_list = sorted(set(new_fake_news_domains))
    # exportListAsCSV(new_fake_news_domains_list, "new_fake_news_domains_list")
    # print("# of Fake News Referrals: " + str(count_of_fake_news_referrals))
    # print("# of Direct Searches or Regular Ad Referrals: " + str(count_of_non_fake_news_referrals))



# main()


def getDeltaTime(t_initial, t_final):
    t_0 = pd.to_datetime(t_initial)
    t_f = pd.to_datetime(t_final)
    return (t_f - t_0)



def examine():
    # List of 600+ domains purchased in court cases
    fake_news_domains_extended = convertListToPyList("fake_domains_list")

    # prior browsing data
    pbd = readDataPKL("prior_browsing_data")
    # Data of rows (out of all the data) where ~25 unique product domains occur
    data_of_relevant_rows = readCSV("csv/","UserIDrelevantspots",["all"])
    compiled_t_deltas = []
    t_initial_df = pd.DataFrame()

    class TimeSet:
        def __init__(self, ti, tf, td):
            self.ti = ti
            self.tf = tf
            self.td = td

    for index, row in data_of_relevant_rows.iterrows():
        # print(str(index) + " " + str(row["machine_id"]))
        rows = pbd.loc[pbd["machine_id"] == row["machine_id"]]
        t_final = row["event_time"]
        # # Find t_initials
        t_initial_list_domain_name = rows.loc[rows["domain_name"].isin(fake_news_domains_extended)]
        t_initial_list_ref_domain_name = rows.loc[rows["ref_domain_name"].isin(fake_news_domains_extended)]

        # compiled_t_initials = t_initial_list_domain_name.append(t_initial_list_ref_domain_name)
        # compiled_t_initials = compiled_t_initials[compiled_t_initials.event_time != t_final]


        # if (compiled_t_initials.empty):
        #     continue
        # else:
        #     t_initials = compiled_t_initials["event_time"].tolist()
        #     for t_initial in t_initials:
        #         t_delta = getDeltaTime(t_initial, t_final)
        #         if (t_delta > timedelta(days=0)):
        #             timeset = TimeSet(t_initial, t_final, t_delta)
        #             compiled_t_deltas.append(timeset)
        #         else:
        #             continue




        # Append to compiled Dataframe
        t_initial_df = t_initial_df.append(t_initial_list_domain_name)
        t_initial_df = t_initial_df.append(t_initial_list_ref_domain_name)
        # Drop t_final duplicates
        # t_initial_df = t_initial_df[t_initial_df.event_time != t_final]




        # t_initials_1 = t_initial_list_domain_name["event_time"].tolist()
        # t_initials_2 = t_initial_list_ref_domain_name["event_time"].tolist()
        # t_initials_compiled = t_initials_1 + t_initials_2
        # for time in t_initials_compiled:
        #     if(time == t_final):
        #         t_initials_compiled.remove(time)
        #     else:
        #         continue
        # if (t_initials_compiled):
        #     print(t_initials_compiled)

    t_initial_df = t_initial_df.drop_duplicates()

    # unique dataframe of fake_news_referrals to product domains initial times
    exportAsCSV("requested_pathway_subset_details", t_initial_df)
    print(len(t_initial_df.index))

    # Print ALL DETAILS FOR TIMESETS
    # for delta in compiled_t_deltas:
    #     print("TI:" + str(delta.ti) + "\n" + "TF:" + str(delta.tf)+ "\n" + "TD:" + str(delta.td) + "\n \n")
    # print(len(compiled_t_deltas))

    # for delta in compiled_t_deltas:
    #     print(str(delta.td))






        # getDeltaTime(rows)






    # pbd = readCSV("csv/", "prior_browsing_data_domain_names", ["all"])
    # exportAsPKL("prior_browsing_data", pbd)


# examine()



def getFakeNewsPrior():
    # List of 600+ domains purchased in court cases
    fake_news_domains_extended = convertListToPyList("fake_domains_list")

    # prior browsing data
    pbd = readDataPKL("prior_browsing_data")

    # Data of rows (out of all the data) where ~25 unique product domains occur
    data_of_relevant_rows = readCSV("csv/","UserIDrelevantspots",["all"])

    export_df = pd.DataFrame()

    for index, row in data_of_relevant_rows.iterrows():
        rows = pbd.loc[pbd["machine_id"] == row["machine_id"]]
        check_domain_names = rows["domain_name"].isin(fake_news_domains_extended)
        check_ref_domain_names = rows["ref_domain_name"].isin(fake_news_domains_extended)
        if (check_domain_names.any() or check_ref_domain_names.any()):
            export_df = export_df.append(rows)
        else:
            continue
        # if (rows["ref_domain_name"].isin(fake_news_domains_extended) o (rows["domain_name"].isin(fake_news_domains_extended)):
        #     print(rows)
        # else;
        #     continue

    exportAsCSV("fake_news_pbd", export_df)



getFakeNewsPrior()
