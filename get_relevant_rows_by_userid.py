from helper import *
from globals import *

def get_relevant_rows_by_userid():
    # all raw data
    all_data = readRawDataPKL(raw_data)

    # product domain list
    product_domains = readCSV("csv/output/"+raw_data+"/", "product_domains_for_" + raw_data, ["all"])
    product_domains_list = product_domains.ix[:,0].values.tolist()

    # columns to check for product domains
    columns = ["domain_name"]

    # product domain list
    filtered_product_domains = readCSV_NoHeaders("csv/output/"+raw_data+"/", "filtered_product_domains_for_" + raw_data, ["all"])
    filter_list = filtered_product_domains[filtered_product_domains.columns[0]].tolist()

    # FILTER ALL DATA TO GET RELEVANT DATA
    relevant_data = getRelevantRows(all_data, columns, filter_list)
    relevant_data = relevant_data.reset_index()
    print(relevant_data)

    # EXPORT DATA
    exportAsCSV("relevant_rows_"+raw_data,"csv/output/"+raw_data+"/",relevant_data)
    if not os.path.exists("pkl/relevant_rows/"+raw_data):
        os.makedirs("pkl/relevant_rows/"+raw_data)
    exportAsPKL("relevant_rows_"+raw_data,"pkl/relevant_rows/"+raw_data+"/",relevant_data)

# Execution Order
get_relevant_rows_by_userid()
