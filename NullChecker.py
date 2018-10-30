"""
Examines a feature class for empty or null values in fields of interest, and by County.

Designed for the Addressing ETL process to provide insight into the data and detect data loss between stages.
User provides the path to the feature class.
Process outputs information to the python console.
Displays total feature count for the feature class, summary stats for each field of interest by Maryland county.
Author: CJuice
Date: 20180222
Revisions: 20180227: Revised to summarize by count and percent for NAME and ZIPCODE by each county.
"""
# TESTING PATHS
# E:\DoIT_AddressingETL_Project\Scripts\TestingPurposes.gdb\PostProcessedStateAddresses
# E:\DoIT_AddressingETL_Project\Scripts\TestingPurposes.gdb\PreProcessedStateAddresses
# E:\DoIT_AddressingETL_Project\Scripts\TestingPurposes.gdb\StateAddresses

# IMPORTS
# import arcpy
from arcpy import Exists
from arcpy import da
from arcpy import env
from arcpy import GetCount_management
import os
from collections import OrderedDict

# VARIABLES (alphabetic)
counties_dict = OrderedDict([('ALLEGANY COUNTY', 0), ('ANNAPOLIS', 0), ('ANNE ARUNDEL COUNTY', 0),
                             ('BALTIMORE CITY', 0), ('BALTIMORE COUNTY', 0), ('CALVERT COUNTY', 0),
                             ('CAROLINE COUNTY', 0), ('CARROLL COUNTY', 0), ('CECIL COUNTY', 0),
                             ('CHARLES COUNTY', 0), ('DORCHESTER COUNTY', 0), ('FREDERICK COUNTY', 0),
                             ('GARRETT COUNTY', 0), ('HARFORD COUNTY', 0), ('HOWARD COUNTY', 0), ('KENT COUNTY', 0),
                             ('MONTGOMERY COUNTY', 0), ('PRINCE GEORGES COUNTY', 0), ('QUEEN ANNES COUNTY', 0),
                             ('SAINT MARYS COUNTY', 0), ('SOMERSET COUNTY', 0), ('TALBOT COUNTY', 0),
                             ('WASHINGTON COUNTY', 0), ('WICOMICO COUNTY', 0), ('WORCESTER COUNTY', 0)])
feature_class_name = None
feature_class_url = None
feature_count = 0
fields_of_interest_dict = OrderedDict([("NAME", 0), ("ZIPCODE", 0), ("ADDSOURCE", 0)])  # ADDSOURCE is in this dict only for the fields list generation step
fields_of_interest_list = fields_of_interest_dict.keys()
NAMEfield_nullcount = 0
NAMEfield_percentnull = 0.0
null_string = 'none'
ZIPCODEfield_nullcount = 0
ZIPCODEfield_percentnull = 0.0

# Instead of pasting repeatedly in dict_counties, I'm adding it with a for loop
for key in counties_dict.keys():
    counties_dict[key] = {"TOTAL": 0, "NAME": 0, "ZIPCODE": 0}

# FUNCTIONS
def calculatepercentnull(recordcount, nullcount):
    if recordcount > 0:
        return ((float(nullcount)/float(recordcount))*100.0)
    else:
        # print("The feature count is zero. Cannot calculate percentage.")
        return -9999

def printcountystats(totalFCrecordcount = feature_count, dictionaryofcounties = counties_dict):
    print("{:^15} {:>7}  {:>7}  {:>7}  {:>7}".format("County", "NameCnt", "%", "ZipCnt", "%"))
    for key in dictionaryofcounties.keys():
        countystripped = key.replace(" COUNTY","")
        # if total is zero then can't do division to get percent
        # print(dict_counties[key])
        if dictionaryofcounties[key]["TOTAL"] > 0:
            print("{:15} {:7d}  {:6.2f}%  {:7d}  {:6.2f}%".format(countystripped, dictionaryofcounties[key]["NAME"], calculatepercentnull(dictionaryofcounties[key]["TOTAL"],dictionaryofcounties[key]["NAME"]), dictionaryofcounties[key]["ZIPCODE"], calculatepercentnull(dictionaryofcounties[key]["TOTAL"],dictionaryofcounties[key]["ZIPCODE"])))
        else:
            print("{:15} {:7d}  {:6.2f}%  {:7d}  {:6.2f}%".format(countystripped, 0, 0.0, 0, 0.0))
    return

def printfieldstats(totalFCrecordcount = feature_count, dictionaryoffields = fields_of_interest_dict):
    print("{:^10}{:>7} {:>7}".format("Field", "Count", "Percent"))
    for key in dictionaryoffields.keys():
        if key != "ADDSOURCE":
            if dictionaryoffields[key] > 0:
                print("{:10}{:7d}{:7.2f}%".format(key, dictionaryoffields[key], calculatepercentnull(totalFCrecordcount,dictionaryoffields[key])))
            else:
                print("{:10}{:7d}{:7.2f}%".format(key, 0, 0.0))
    return

# FUNCTIONALITY
    # Try: Get the location of the feature class we are going to process
try:
    stayinloop = True
    while stayinloop:
        feature_class_url = raw_input("Paste the path to the feature class\n>")
        if os.path.exists((os.path.dirname(feature_class_url))):
            stayinloop = False
        else:
            print("There is a problem with the path to the feature class.\n\t"
                  "Note, the FC is expected to be sitting in the root gdb, not in a feature dataset.")
except:
    exit()
print("...")
    # Try:
        # Check feature class exists and get feature count
try:
    env.workspace = os.path.dirname(feature_class_url)
    tempFCname = os.path.basename(feature_class_url)
    if Exists(tempFCname):
        feature_class_name = tempFCname
        print("Getting feature count...")
        result = GetCount_management(feature_class_name)
        feature_count = int(result[0])
        print("\n\tTOTAL = {:,}\n".format(feature_count))
    else:
        exit()
except:
    print("Feature class does not appear to exist")
    exit()

    # Try: Establish a cursor
    # Step through critical fields and keep count of number of "null" values. "null" values show as type NoneType.
    #   When converted to string NoneType shows as "None".
# stopcount = 0
try:
    print("Getting null/empty count...\n")
    with da.SearchCursor(feature_class_url, fields_of_interest_list) as cursor:
        for row in cursor:
            localcounter = 0
            # CJUICE: testing limiter left in code for future needs
            # if stopcount < 50:
            name = (str(row[0]).strip()).lower()
            zipcode = (str(row[1]).strip()).lower()
            namelength = len(name)
            zipcodelength = len(zipcode)
            if name == null_string or namelength == 0:
                fields_of_interest_dict["NAME"] += 1
                counties_dict[row[2]]["NAME"] += 1
            if zipcode == null_string or zipcodelength == 0:
                fields_of_interest_dict["ZIPCODE"] += 1
                counties_dict[row[2]]["ZIPCODE"] += 1
            counties_dict[row[2]]["TOTAL"] += 1
            #     stopcount += 1
            # else:
            #     break
except:
    print("Error while accessing cursor on feature class attribute table")

# Calculate and print out stats
printfieldstats(feature_count)
print("\n")
printcountystats(feature_count)