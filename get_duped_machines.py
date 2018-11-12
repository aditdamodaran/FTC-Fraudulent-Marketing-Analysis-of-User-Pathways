from helper import *
from globals import *

# INPUT:
# KNOWING RELEVANT ROWS
# HAVING CUT DOWN ALL RAW DATA TO JUST DATA OF DUPED MACHINES (TO MAKE THINGS QUICKER)

# OUTPUT:
# Filter raw data to just the browsing histories of duped machines

def getDupedMachines():
    # all raw data
    all_data = readRawDataPKL(raw_data)

    # relevant rows
    relevant_rows = readDataPKL("pkl/relevant_rows/"+raw_data+"/", "relevant_rows_"+raw_data)

    # unique list of duped machine IDs
    machine_ids = relevant_rows["machine_id"].tolist()
    machine_ids = sorted(set(machine_ids))
    exportListAsCSV(machine_ids, "csv/output/"+raw_data+"/","duped_machine_ids_for_" + raw_data)

    duped_machine_browsing_history = all_data.loc[all_data["machine_id"].isin(machine_ids)]

    # EXPORT DATA
    exportCSVandPKL("duped_machines", duped_machine_browsing_history)

getDupedMachines()
