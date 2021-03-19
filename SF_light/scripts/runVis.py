from keplergl import KeplerGl
import json
import cordonConvert

def createVisual (dataFile, configFile, outputFile, cordonFile=None):
    data = {}
    config = {}
    cordon = {}
    
    # If json, parse as json
    if "json" in dataFile.split(".")[-1]:
        with open (dataFile, "r") as fileIn:
            data = json.load(fileIn)
    else:
        # Else, it's CSV
        with open (dataFile, "r") as fileIn:
            cols = fileIn.readline().split(",")
            lines = fileIn.readlines()
            for i, col in enumerate(cols):
                data[col] = list(map(lambda x: x.split(',')[i], lines))
    
    with open (configFile, "r") as fileIn:
        config = json.load(fileIn)
    
    # This one is in a weird format, so it's a bit more difficult
    fileOut = cordonConvert.runConvert(cordonFile)
    
    with open(fileOut, "r") as fileRead:
        cordon = json.load(fileRead)
    
    # Now create kepler map
    kmap = KeplerGl(height=600, width=800)
    kmap.config = config
    kmap.add_data(data=cordon, name="cordon")
    kmap.add_data(data=data, name="data")
    kmap.save_to_html(file_name=outputFile)

fileName = ["costsByZone_end_TAZ",
            "costsByZone_Start_TAZ",
            "modeShare_TAZ",
            "occupancyByZone_TAZ",
            "timeDelayByZone_TAZ",
            "totalDistance_TAZ",
            "travelTimes",
            "tripDensityByZone_TAZ"
            ]
for i in range(8):
    createVisual("../output_files/01729e42-41cd-11eb-94f5-9801a798306b/{}.json".format(fileName[i]), "../input_files/configs/{}.json".format(fileName[i]), "../visualizations/temp/{}.html".format(fileName[i]), "../input_files/circle_params.txt")