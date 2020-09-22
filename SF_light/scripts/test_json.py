import shapefile
from json import dumps

SHAPEFILE_PATH = "../input_files/shapefiles/SF_Neighborhoods/neighborhoods.shp"
OUTPUT_PATH = "../input_files/shapefiles/SF_Neighborhoods/shape.json"

# read the shapefile
reader = shapefile.Reader(SHAPEFILE_PATH)
fields = reader.fields[1:]
blacklistIDs = [103970, 104467]
field_names = [field[0] for field in fields]
buffer = []
for sr in reader.shapeRecords():
	atr = dict(zip(field_names, sr.record))
	# if (int(atr["COUNTYFP10"]) == 75 and not int(atr["TAZCE10"]) in blacklistIDs):
	geom = sr.shape.__geo_interface__
	buffer.append(dict(type="Feature", \
	geometry=geom, properties=atr)) 

# write the GeoJSON file

geojson = open(OUTPUT_PATH, "w")
geojson.write(dumps({"type": "FeatureCollection", "features": buffer}, indent=2) + "\n")
geojson.close()