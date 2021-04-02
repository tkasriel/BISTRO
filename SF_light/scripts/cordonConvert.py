import utm, math, json
# Small script to convert cordon params into coords.
# INPUT_FILE = "../input_files/circle_params.txt"
OUTPUT_FILE = "../output_files/circle_params.json"

# Region for UTM-WGS84 > lat/long conversion
ZONE_NUM = 14
ZONE_LETTER = "N"
def runConvert(INPUT_FILE):
    coords = []
    with open (INPUT_FILE, "r") as fileIn:
        coords = fileIn.readline().split(',')[:-1] # circle_params is a single line

    # We need to make it into geoJSON in order for kepler to accept it
    outputJSON = {}
    outputJSON["type"] = "FeatureCollection"
    outputJSON["features"] = []


    for i in range(0, len(coords), 4): # X, Y, Rad, Cost
        # Extract values:
        x = float(coords[i].split(":")[1])
        y = float(coords[i+1].split(":")[1])
        r = float(coords[i+2].split(":")[1])
        p = float(coords[i+3].split(":")[1])
        center = (x, y)
        side = (x+r, y) # We'll need to use this to get the radius of the circle
        
        centerOut = utm.to_latlon(center[0], center[1], ZONE_NUM, ZONE_LETTER)
        sideOut = utm.to_latlon(side[0], side[1], ZONE_NUM, ZONE_LETTER)

        # We'll fake a circle by making a regular N-sided polygon
        sides = 20
        pts = []
        radius = math.sqrt((sideOut[0] - centerOut[0]) ** 2 + (sideOut[1] - centerOut[1]) ** 2)
        for j in range(sides+1):
            lon = centerOut[0] + radius * math.cos(j * 2 * math.pi / sides)
            lat = centerOut[1] + radius * math.sin(j * 2 * math.pi / sides)
            pts.append((lat, lon))
        
        # Add circle to output dict
        outputJSON["features"].append({})
        poly = outputJSON["features"][-1]
        poly["type"] = "Feature"
        poly["geometry"] = {}
        poly["properties"] = {}
        poly["properties"]["cost"] = p

        # Geometry for circle
        geo = poly["geometry"]
        geo["type"] = "Polygon"
        geo["coordinates"] = [pts]


    with open(OUTPUT_FILE, "w") as fileOut:
        fileOut.write(json.dumps(outputJSON))
    return OUTPUT_FILE