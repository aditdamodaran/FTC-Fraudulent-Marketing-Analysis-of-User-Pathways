from helper import *
from globals import *

def getProductDomains():
    # all raw data
    all_data = readRawDataPKL(raw_data)

    # list of all fake news domains (+ extended list)
    extended_list_of_fake_news_domains_600

    fake_news_domains_as_referrals = all_data[all_data['ref_domain_name'].isin(extended_list_of_fake_news_domains_600)]

    product_domains = fake_news_domains_as_referrals['domain_name'].tolist()
    product_domains = set(sorted(product_domains))

    if not os.path.exists("csv/output/"+raw_data):
        os.makedirs("csv/output/"+raw_data)

    exportListAsCSV(product_domains, "csv/output/"+raw_data+"/", "product_domains_for_" + raw_data)

# Execution Order
getProductDomains()
