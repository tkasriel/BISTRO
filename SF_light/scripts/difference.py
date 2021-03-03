'''
    This script serves to find the difference between corresponding visualizations of two different simulation runs
'''
import os, sys, json, copy
WHITELIST = ["avgCost", "totCost", "walk", "bus", "bike", "walk_transit", "drive_transit", "ridehail_transit", "car",
                                    "walk_percentage", "bus_percentage", "bike_percentage", "walk_transit_percentage", "drive_transit_percentage", "ridehail_transit_percentage", "car_percentage", "modal_percentage", "modal_percentage_hidden",
                                    "totalVehicleOccupancy", "avgVehicleOccupancy",
                                    "total distance",
                                    "timeDelay",
                                    "avgTimeTo", "ttotTime", "avgTimeFrom", "ftotTime", "avgTime"]
BASE_ID = "f4c0a90c-7802-11eb-947d-06b502d4a7f7"
COMP_ID = "32cfbb84-39ce-11eb-9ec7-9801a798306b"
FILE_NAMES = ["costsByZone_end_TAZ.json","costsByZone_Start_TAZ.json","modeShare_TAZ.json","occupancyByZone_TAZ.json","timeDelayByZone_TAZ.json","totalDistance_TAZ.json","travelTimes.json","tripDensityByZone_TAZ.json"]
OUTPUT_FOLDER = "../output_files/differences/{}::{}".format(BASE_ID, COMP_ID)
try:
	os.mkdir(OUTPUT_FOLDER)
except:
	#dir already exists
	pass


def differenceJson (jsonBase, jsonComp):
    ''' Compare JSON outputs
    '''
    jsonOutput = copy.deepcopy (jsonBase)
    
    baseElems = jsonBase["features"]
    compElems = jsonComp["features"]
    outputElems = jsonOutput["features"]

    #Goal: iterate through elements & compare each one
    '''
    Prereqs:
        Both are the same visualization 
        All properties that should be modified need to be included in the WHITELIST list
    '''
    for i in range(len((baseElems))):
        for prop in WHITELIST:
            if not (prop in baseElems[i]["properties"]):
                continue
            try:
                if '%' in str(baseElems[i]["properties"][prop]):
                    outputElems[i]["properties"][prop] = str(int(baseElems[i]["properties"][prop][:-1]) - int(compElems[i]["properties"][prop][:-1])) + "%"
                else:
                    outputElems[i]["properties"][prop] = float(baseElems[i]["properties"][prop]) - float(compElems[i]["properties"][prop])
            except Exception as e:
                # print (prop + " : " + str(baseElems[i]["properties"][prop]) + " - " + str(compElems[i]["properties"][prop]))
                # if len(str(baseElems[i]["properties"][prop])) > 0 and len(str(compElems[i]["properties"][prop])) > 0:
                #     raise Exception (e)
                outputElems[i]["properties"][prop] = None # One of these values is NaN

    with open ("{}/{}".format(OUTPUT_FOLDER, FILE_NAME), "w") as outFile:
        json.dump(jsonOutput, outFile, allow_nan=True)

def differenceCSV (csvBase, csvComp):
    ''' Compare CSV outputs
    '''
    
    '''
    Prereqs:
        Both are the same visualization 
        All properties that should be modified need to be included in the WHITELIST list
        For all row R, both csv inputs associate the same data with that row
    '''
    outputCSV = [[]]
    outputCSV[0] = csvBase[0][:] # Copy column headers

    for row in range(1, len((csvBase))):
        outputCSV.append([])
        for col in range(len(csvBase[row])):
            if outputCSV[0][col] in WHITELIST: # If property is in the whitelist
                outputCSV[row].append(csvBase[row][col] - csvComp[row][col]) 
            else:
                outputCSV[row].append(csvBase[row][col])

    with open ("../output_files/difference.csv", "w") as outFile:
        for row in outputCSV:
            for col in row:
                outFile.write(col + ",")
            outFile.write("\n")

baseJson, compJson = ({}, {})


for FILE_NAME in FILE_NAMES:
    with open ("../output_files/{}/{}".format(BASE_ID, FILE_NAME), "r") as inFile:
        baseJson = json.load(inFile)

    with open ("../output_files/{}/{}".format(COMP_ID, FILE_NAME), "r") as inFile:
        compJson = json.load(inFile)
    differenceJson(baseJson, compJson)