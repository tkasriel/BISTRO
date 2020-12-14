import shapefile
from json import dumps

SHAPEFILE_PATH = "../input_files/shapefiles/City_of_Sioux_Falls_City_Limits/City_of_Sioux_Falls_City_Limits.shp"
OUTPUT_PATH = "../input_files/shapefiles/City_of_Sioux_Falls_City_Limits/City_of_Sioux_Falls_City_Limits.json"

# read the shapefile
reader = shapefile.Reader(SHAPEFILE_PATH)
fields = reader.fields[1:]
# whitelistCounties = [83, 99]
# blacklistIDs = [[10400, 10108, 10107, 10300, 10200], [10402, 10300, 10200, 10502, 10501, 10101, 10102]]
field_names = [field[0] for field in fields]
buffer = []
for sr in reader.shapeRecords():
	atr = dict(zip(field_names, sr.record))
	# if int(atr["COUNTYFP10"]) in whitelistCounties:
	# 	include = True
	# 	for i in range(len(whitelistCounties)):
	# 		if int(atr["TAZCE10"]) in blacklistIDs[i] and int(atr["COUNTYFP10"]) == whitelistCounties[i]:
	# 			include = False
	# 			break
	# 	if include:
	geom = sr.shape.__geo_interface__
	buffer.append(dict(type="Feature", \
	geometry=geom, properties=atr)) 

# write the GeoJSON file

geojson = open(OUTPUT_PATH, "w")
geojson.write(dumps({"type": "FeatureCollection", "features": buffer}, indent=2) + "\n")
geojson.close()