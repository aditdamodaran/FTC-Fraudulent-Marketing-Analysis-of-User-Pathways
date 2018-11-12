from helper import *
from globals import *
from datetime import datetime, time as datetime_time, timedelta

# Remove Warning for Sorting Browsing History by Date & Time
pd.options.mode.chained_assignment = None  # default='warn'

# INPUT:
# KNOWING RELEVANT ROWS
# HAVING CUT DOWN ALL RAW DATA TO JUST DATA OF DUPED MACHINES (TO MAKE THINGS QUICKER)

# OUTPUT:

# Check that a time_0 is within an interval before time_f
def withinInterval(time_0, time_f, interval):
    time_delta = str(datetime.strptime(time_f,"%H:%M:%S") - datetime.strptime(time_0,"%H:%M:%S"))
    time_delta = time_delta.split(":")

    interval = interval.split(":")

    try:
        # Check that timesteamp is within specified hour
        if (int(interval[2]) >= int(time_delta[2])):
            # Check that timesteamp is within specified minute
            if(int(interval[1]) >= int(time_delta[1]) ):
                # Check that timesteamp is within specified seconds
                if(int(interval[0]) >= int(time_delta[0])):
                    return 1
                    # Within Interval
                else:
                    return 0
            else:
                return 0
        else:
            return 0
    except:
        return 0

def extractPBD():
    # duped machine browsing data
    duped_machine_browsing_history = readDataPKL("pkl/duped_machines/"+raw_data+"/", "duped_machines_"+raw_data)

    # relevant rows
    relevant_rows = readDataPKL("pkl/relevant_rows/"+raw_data+"/", "relevant_rows_"+raw_data)

    # duped machine ids
    duped_machine_ids = readCSV_NoHeaders("csv/output/"+raw_data+"/", "duped_machine_ids_for_" + raw_data, ["all"])
    duped_machine_ids = duped_machine_ids[duped_machine_ids.columns[0]].tolist()

    # compiled prior browsing data in single dataframe for export
    compiled_pbd = pd.DataFrame()

    # GET PRIOR BROWSING DATA WITHIN SPECIFIED INTERVAL
    interval = "00:00:20"
    for machine_id in duped_machine_ids:
         rows = duped_machine_browsing_history.loc[duped_machine_browsing_history["machine_id"] == machine_id]

         # ----------- Sort Browsing History by Date and Time Respectively
         rows['Date'] = pd.to_datetime(rows['event_date'])
         rows['Time'] = pd.to_datetime(rows['event_time'])
         rows = rows.sort_values(['Date','Time'])

         # ------------ SORTED BROWSING HISTORY OF DUPED MACHINES  ------------
         # print(rows[['machine_id', 'event_time','event_date']])
         # ------------ ------------ ------------ ------------ ------------ ---


         # first find the common values
         common_rows = pd.merge(relevant_rows, rows, on=["machine_id","event_time","event_date"])
         relevant_dates_list = []
         for index, row in common_rows.iterrows():
            relevant_dates_list.append(str(row["event_date"]))
         # List of dates (for each machine_id) of instances in which a product domain was accessed
         # Note: +1 instance of accessing a product domain may happen on each date
         relevant_dates_list = sorted(set(relevant_dates_list))

         for date in relevant_dates_list:
              # Objective: for each relevant date,
              # find each relevant_row (instance of accessing a product domain instance)

              # for each machine_id, for each relevant date (product domain accessed), get the browsing history
              # formerly relevant_rows_by_date
              relevant_browsing_history = rows[(rows.event_date == date)]

              # for each date,
              # get the rows for instances of accessing a product domain
              # formerly relevant_timestamps_lists
              relevant_instances = pd.merge(common_rows, relevant_browsing_history, on=["event_date", "event_time"])

              for index, row in relevant_instances.iterrows():
                  # timestamp as String of each relevant event
                  event_timestamp = row["event_date"]
                  event_timestamp_string = getTimestampAsString(row)

                  # limit browsing history to only user activity BEFORE getting to product domain
                  # add "=" to the below statement after '<' to include the product domain itself
                  prior_browsing_data = relevant_browsing_history.loc[(relevant_browsing_history["event_time"]<=event_timestamp_string)]

                  # clean data to specified interval
                  # browsing history within a time interval specified above prior to arriving at a product domain
                  for index, row in prior_browsing_data.iterrows():
                      time_0 = getTimestampAsString(row)
                      time_f = event_timestamp_string
                      if (withinInterval(time_0, time_f, interval)):
                          continue
                      else: # drop the data *after* the user arrived at the product domain
                          prior_browsing_data = prior_browsing_data.drop([index])

                  # Compiled Browsing Data
                  compiled_pbd = compiled_pbd.append(prior_browsing_data)


    # EXPORT COMPILED PRIOR BROWSING DATA
    exportCSVandPKL("compiled_prior_browsing_data_20secs", compiled_pbd)





extractPBD()
