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


def main():
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
    # feature_class_name = None
    feature_class_url = None
    feature_count = 0
    fields_of_interest_dict = OrderedDict([("NAME", 0), ("ZIPCODE", 0), ("ADDSOURCE", 0)])  # ADDSOURCE is in this dict only for the fields list generation step
    fields_of_interest_list = list(fields_of_interest_dict.keys())
    # NAMEfield_nullcount = 0
    # NAMEfield_percentnull = 0.0
    null_string = 'none'
    # ZIPCODEfield_nullcount = 0
    # ZIPCODEfield_percentnull = 0.0

    # Instead of pasting repeatedly in dict_counties, I'm adding it with a for loop
    for key in counties_dict.keys():
        counties_dict[key] = {"TOTAL": 0, "NAME": 0, "ZIPCODE": 0}

    # FUNCTIONS
    def calculate_percent_null(recordcount, nullcount):
        """

        :param recordcount:
        :param nullcount:
        :return:
        """
        if recordcount > 0:
            return ((float(nullcount)/float(recordcount))*100.0)
        else:
            # print("The feature count is zero. Cannot calculate percentage.")
            return -9999

    def print_county_stats(dict_of_counties=counties_dict):
        print("{:^15} {:>7}  {:>7}  {:>7}  {:>7}".format("County", "NameCnt", "%", "ZipCnt", "%"))
        for key in dict_of_counties.keys():
            county_stripped = key.replace(" COUNTY", "")
            # if total is zero then can't do division to get percent
            # print(dict_counties[key])
            if dict_of_counties[key]["TOTAL"] > 0:
                print("{:15} {:7d}  {:6.2f}%  {:7d}  {:6.2f}%".format(county_stripped,
                                                                      dict_of_counties[key]["NAME"],
                                                                      calculate_percent_null(
                                                                          dict_of_counties[key]["TOTAL"],
                                                                          dict_of_counties[key]["NAME"]),
                                                                      dict_of_counties[key]["ZIPCODE"],
                                                                      calculate_percent_null(
                                                                          dict_of_counties[key]["TOTAL"],
                                                                          dict_of_counties[key]["ZIPCODE"])
                                                                      )
                      )
            else:
                print("{:15} {:7d}  {:6.2f}%  {:7d}  {:6.2f}%".format(county_stripped, 0, 0.0, 0, 0.0))
        return

    def printfieldstats(total_FC_record_count=feature_count, dict_of_fields=fields_of_interest_dict):
        print("{:^10}{:>7} {:>7}".format("Field", "Count", "Percent"))
        for key in dict_of_fields.keys():
            if key != "ADDSOURCE":
                if dict_of_fields[key] > 0:
                    print("{:10}{:7d}{:7.2f}%".format(key, dict_of_fields[key], calculate_percent_null(
                        total_FC_record_count,
                        dict_of_fields[key]))
                          )
                else:
                    print("{:10}{:7d}{:7.2f}%".format(key, 0, 0.0))
        return

    # FUNCTIONALITY
        # Try: Get the location of the feature class we are going to process
    try:
        stay_in_loop = True
        while stay_in_loop:
            feature_class_url = input("Paste the path to the feature class\n>")
            if os.path.exists((os.path.dirname(feature_class_url))):
                stay_in_loop = False
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
        temp_FC_name = os.path.basename(feature_class_url)
        if Exists(temp_FC_name):
            feature_class_name = temp_FC_name
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
        with da.SearchCursor(in_table=feature_class_url, field_names=fields_of_interest_list) as cursor:
            for row in cursor:
                # localcounter = 0
                # CJUICE: testing limiter left in code for future needs
                # if stopcount < 50:
                name = (str(row[0]).strip()).lower()
                zipcode = (str(row[1]).strip()).lower()
                namelength = len(name)
                zipcode_length = len(zipcode)
                if name == null_string or namelength == 0:
                    fields_of_interest_dict["NAME"] += 1
                    counties_dict[row[2]]["NAME"] += 1
                if zipcode == null_string or zipcode_length == 0:
                    fields_of_interest_dict["ZIPCODE"] += 1
                    counties_dict[row[2]]["ZIPCODE"] += 1
                counties_dict[row[2]]["TOTAL"] += 1
                    # stopcount += 1
                # else:
                #     break
    except Exception as e:
        print(f"Error while accessing cursor on feature class attribute table: \n{e}")

    # Calculate and print out stats
    printfieldstats(feature_count)
    print("\n")
    print_county_stats()


if __name__ == "__main__":
    main()