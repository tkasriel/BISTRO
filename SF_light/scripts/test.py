DATA_FILE = "../input_files/sf_light_50k_BAU_link_stats_by_hour.csv"
OUTPUT_FOLDER = "../output_files/"
NEIGHBOR_ZONE_FILE = "../input_files/shapefiles/SF_Neighborhoods/shape.json" #SF_Neighborhoods // TAZ_SF
TAZ_ZONE_FILE = "../input_files/shapefiles/TAZ_SF/shape.json"
MODES = "walk, bus, bike, walk_transit, drive_transit, ridehail_transit, ride_hail, ridehail_pooled, car".split(", ")
MODE_GROUPS = "active, car, ridehail, transit".split(", ") #pedestrian, car, ridehail, public transport
MODE_GROUP_DICT = {"walk": 0, "bike": 0, "walk_transit": 3, "drive_transit": 3, "ridehail_transit": 3, "ride_hail": 2, "ridehail_pooled":2, "car":1, "bus": 3}
SIMUL_ID = "db21069e-d19b-11ea-bfff-faffc250aee5" #db21069e-d19b-11ea-bfff-faffc250aee5
TIME_SEP = 14400 # in seconds
INCOME_SEP = 50000 # 0 - 200 000
NUM_THREADS = 4
import os, sys, threading, copy, json, math
sys.path.append(os.path.abspath("/Users/git/BISTRO_Dashboard/BISTRO_Dashboard"))
os.chdir('/Users/git/BISTRO_test/SF_light/scripts') # Cause VScode is weird

from db_loader import BistroDB
import numpy as np
import pandas as pd
import shapely.geometry as geo

# Are these pointless? yes
# Am I still going to keep them? yes
def loadDB (simulation_id):
	database = BistroDB('bistro', 'bistroclt', 'client', '52.53.200.197')
	simulation_id = [simulation_id]
	scenario = database.get_scenario(simulation_id)
	return (database, scenario, simulation_id)
def loadLegs(db, simulation_id):
	print("Loading legs...")

	# f = open("../input_files/legs.csv", "r")
	# legs = {}
	# vals = []
	# for i, line in enumerate(f.readlines()):
	# 	l = line.split(",")
	# 	for j, val in enumerate(l):
	# 		if i == 0:
	# 			legs[val] = []
	# 			vals.append(val)
	# 		else:
	# 			legs[vals[j]].append(val)
		
	legs = db.load_legs(simulation_id, links=True)
	print("legs loaded")
	return legs
def loadAct(db, scenario):
	print("Loading activities...")
	# acts = db.load_activities(scenario)
	'ind,pid,act_id,act_type,act_link,act_start_time,act_end_time,scenario'
	df_cols = ["index", "PID", "ActNum", "Type", "LinkId", "Start_time", "End_time", "scenario"]
	acts = pd.read_csv('../input_files/sf_light_100k_simple_network_act.csv', header=0, names=df_cols)
	print("activities loaded")
	return acts
def loadFacilities(db, scenario):
	print("Loading facilities...")
	facs = db.load_facilities(scenario)
	print("facilities loaded")
	return facs
def loadLinks(db, scenario):
	print("Loading links...")
	links = db.load_links(scenario)
	print("links loaded")
	return links
def loadPaths(db, simulation_id, scenario):
	print("Loading paths...")
	paths = db.load_paths(simulation_id, scenario)
	print("paths loaded")
	return paths
def loadTrips (db, simulation_id):
	print("Loading trips...")

	# f = open("../input_files/trips.csv", "r")
	# trips = {}
	# vals = []
	# for i, line in enumerate(f.readlines()):
	# 	l = line.split(",")
	# 	for j, val in enumerate(l):
	# 		if i == 0:
	# 			trips[val] = []
	# 			vals.append(val)
	# 		else:
	# 			trips[vals[j]].append(val)
	
	trips = db.load_trips(simulation_id)
	print("trips loaded")
	return trips
def loadPopulation(db, scenario):
	print("Loading population...")
	people = db.load_person(scenario)
	print("population loaded")
	return people
def loadVehicles (db, scenario):
	print("Loading vehicles...")
	vehicles = db.load_vehicles(scenario)
	print("vehicles loaded")
	return vehicles
def loadVehicleTypes (db, scenario):
	print("Loading vehicles types...")
	vehicles = db.load_vehicle_types(scenario)
	print("vehicles types loaded")
	return vehicles
def loadNodes (db, scenario):
	print("Loading nodes...")
	nodes = db.load_nodes(scenario)
	print("nodes loaded")
	return nodes

# Generic class to call a function on multiple threads
class processingThread (threading.Thread):
	def __init__(self, threadID, name, runFunction, **kwargs):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.name = name
		self.func = runFunction
		self.kwargs = kwargs
	def run(self):
		print(self.name + ": Starting process")
		self.out = self.func(**self.kwargs)
		print(self.name + ": Process finished")

# While this makes one individual visualization much slower, it means that I can run all of them much faster (each can be disabled if you don't want to waste time grabbing useless tables)
(database, scenario, simulation_id) = loadDB(SIMUL_ID)
legs = loadLegs(database, simulation_id)
links = loadLinks(database, scenario)
trips = loadTrips(database, simulation_id)
population = loadPopulation(database, scenario)
vehicles = loadVehicles(database, scenario)
activities = loadAct(database, scenario)
facilities = loadFacilities(database, scenario)
vehicleTypes = loadVehicleTypes(database, scenario)
paths = loadPaths(database, simulation_id, scenario)
nodes = loadNodes(database, scenario)
print("Table queries finished")

with open(NEIGHBOR_ZONE_FILE, "r") as neighborZoneFile:
	neighborZones = json.load(neighborZoneFile)
with open(TAZ_ZONE_FILE, "r") as TAZZoneFile:
	TAZZones = json.load(TAZZoneFile)
with open(DATA_FILE, "r") as speedFile:
	speeds = speedFile.readlines()
print("Files opened")

def travelTimesByZone():
	global legs, links, zones, nodes
	print ("Creating visual: travelTimesByZone")
	zones = {}
	print ("Copying zone information..")
	zones = copy.deepcopy(neighborZones)
	polys = []
	for i, poly in enumerate(zones["features"]):
		# print(poly["geometry"]["coordinates"][0])
		polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
		poly["properties"]["focusedNode"] = "all"
		poly["properties"]["mode"] = MODES[0]
		poly["properties"]["ttotTime"] = 0
		poly["properties"]["tnumNodes"] = 0
		poly["properties"]["avgTimeTo"] = None
		poly["properties"]["ftotTime"] = 0
		poly["properties"]["fnumNodes"] = 0
		poly["properties"]["avgTimeFrom"] = None
		poly["properties"]["avgTime"] = None
		poly["properties"]["ind"] = i
		poly["properties"]["time"] = 1

	#Duplicate map for every node
	index = len(zones["features"])
	for t in range(math.ceil(24 * 60 * 60 / TIME_SEP)+1):
		for o in range(len(MODES)):
			for i in range(len(polys)+1):
				if t == 0 and o == 0 and i == 0:
					continue
				for j in range(len(polys)):
					poly = zones["features"][j]
					zones["features"].append(copy.deepcopy(poly))
					if i == 0:
						zones["features"][index]["properties"]["focusedNode"] = "all"
					else:
						zones["features"][index]["properties"]["focusedNode"] = zones["features"][i-1]["properties"]["name"]
					zones["features"][index]["properties"]["mode"] = MODES[o]
					zones["features"][index]["properties"]["ind"] = index
					zones["features"][index]["properties"]["time"] = t+1
					index += 1
					if index % 1000 == 0:
						print ("Creating zones: " + str(index - len(polys)) + " / " + str(len(polys) * len(polys) * len(MODES) * math.ceil(24 * 60 * 60 / TIME_SEP)))
	#Create links
	linkMap = [[] for i in range(100000)]
	for i in range(len(links["LinkId"])):
		linkMap[int(links["LinkId"][i])] = [int(links["fromLocationID"][i]), int(links["toLocationID"][i])]
	nodeMap = [[] for i in range(100000)]
	for i in range(len(nodes["NodeId"])):
		nodeMap[int(nodes["NodeId"][i])] = [nodes["x"][i], nodes["y"][i]]


	#Parse input per leg
	ignored = 0
	ignored_modes = []
	for i in range(len(legs["PID"])):
		# for i in range(1):
		if i % 1000 == 0:
			print("Parsing legs: " + str(i) + " / " + str(len(legs["LinkId"])))
			pass
		leg_links = legs["LinkId"][i]
		#Find start and end pt
		st = 0
		end = 0
		try:
			if linkMap[int(float(leg_links[0]))][0] in linkMap[int(float(leg_links[1]))]:
				st = linkMap[int(float(leg_links[0]))][1]
			else:
				st = linkMap[int(float(leg_links[0]))][0]
			if linkMap[int(float(leg_links[-1]))][0] in linkMap[int(float(leg_links[-2]))]:
				end = linkMap[int(float(leg_links[-1]))][1]
			else:
				end = linkMap[int(float(leg_links[-1]))][0]
		except:
			ignored+=1
		stPt = geo.Point(nodeMap[st])
		endPt = geo.Point(nodeMap[end])
		stInd = 0
		endInd = 0
		for j, poly in enumerate(polys):
			if stPt.within(poly):
				stInd = j
			if endPt.within(poly):
				endInd = j

		#Debug purposes
		# stInd = 0
		# endInd = 2
		# modeFactor = 3

		try:
			modeFactor = MODES.index(legs["Mode"][i])
		except:
			ignored+=1
			if not (legs["Mode"][i] in ignored_modes):
				ignored_modes.append(legs["Mode"][i])
			continue
		timeFactor = ((int(legs["End_time"][i]) + int(legs["Start_time"][i])) // (2 * TIME_SEP))
		ind = timeFactor * len(MODES) * (len(polys)+1) * len(polys) + modeFactor * (len(polys)+1) * len(polys) + (endInd+1) * len(polys) + stInd
		all_modeInd = timeFactor * len(MODES) * (len(polys)+1) * len(polys) + (endInd+1) * len(polys) + stInd
		all_fromInd = timeFactor * len(MODES) * (len(polys)+1) * len(polys) + modeFactor * (len(polys)+1) * len(polys) + stInd
		all_allInd = timeFactor * len(MODES) * (len(polys)+1) * len(polys) + stInd 
		inds = [ind, all_modeInd, all_fromInd, all_allInd]
		# print("Start: " + str(stInd) + ", End: " + str(endInd) + " --> " + str(ind))
		time = int(legs["End_time"][i]) - int(legs["Start_time"][i])
		for o, index in enumerate(inds):
			zones["features"][index]["properties"]["ttotTime"] += time
			zones["features"][index]["properties"]["tnumNodes"] += 1
		ind = timeFactor * len(MODES) * (len(polys)+1) * len(polys) + modeFactor * (len(polys)+1) * len(polys) + (stInd+1) * len(polys) + endInd
		all_modeInd = timeFactor * len(MODES) * (len(polys)+1) * len(polys) + (stInd+1) * len(polys) + endInd
		all_fromInd = timeFactor * len(MODES) * (len(polys)+1) * len(polys) + modeFactor * (len(polys)+1) * len(polys) + endInd
		all_allInd = timeFactor * len(MODES) * (len(polys)+1) * len(polys) + endInd 
		inds = [ind, all_modeInd, all_fromInd, all_allInd]
		for o, index in enumerate(inds):
			zones["features"][index]["properties"]["ftotTime"] += time
			zones["features"][index]["properties"]["fnumNodes"] += 1
		
	print ("Legs parsed: " + str(len(legs["PID"]) - ignored))
	print("Legs ignored: " + str(ignored))
	print ("Modes ignored: " + ", ".join(ignored_modes))

	for i, poly in enumerate(zones["features"]):
		if poly["properties"]["tnumNodes"] > 0:
			poly["properties"]["avgTimeTo"] = round(poly["properties"]["ttotTime"] / poly["properties"]["tnumNodes"] * 10) / 10
		if poly["properties"]["fnumNodes"] > 0:
			poly["properties"]["avgTimeFrom"] = round(poly["properties"]["ftotTime"] / poly["properties"]["fnumNodes"] * 10) / 10
		if poly["properties"]["tnumNodes"] + poly["properties"]["fnumNodes"] > 0:
			poly["properties"]["avgTime"] = round((poly["properties"]["ttotTime"] + poly["properties"]["ftotTime"]) / (poly["properties"]["tnumNodes"] + poly["properties"]["fnumNodes"]) * 10) / 10
	with open (OUTPUT_FOLDER+"/travelTimes.json", "w") as out:
		json.dump(zones, out, allow_nan=True)
def costsByZone (isTAZ):
	global trips, links, population, legs, nodes
	print ("Creating visual: costsByZone. IsTAZ = " + str(isTAZ))
	polys = []
	zones = {}
	print ("Copying zone information..")
	if isTAZ:
		zones = copy.deepcopy(TAZZones)
	else:
		zones = copy.deepcopy(neighborZones)
	for i, poly in enumerate(zones["features"]):
		# print(poly["geometry"]["coordinates"][0])
		polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
		poly["properties"]["mode"] = MODES[0]
		poly["properties"]["totCost"] = 0
		poly["properties"]["numVals"] = 0
		poly["properties"]["avgCost"] = None
		poly["properties"]["ind"] = i
		if isTAZ:
			poly["properties"]["income"] = "0-" + str(INCOME_SEP)
			poly["properties"]["time"] = "0:00:00"
		else:
			poly["properties"]["focusedNode"] = "all"

	index = len(zones["features"])
	if isTAZ:
		for t in range(math.ceil(24 * 60 * 60 / TIME_SEP)):
			for inc in range(math.ceil(200000 / INCOME_SEP)):
				for o in range(len(MODES)):
					if t == 0 and inc == 0 and o == 0:
						continue
					for j in range(len(polys)):
						poly = zones["features"][j]
						zones["features"].append(copy.deepcopy(poly))
						zones["features"][index]["properties"]["mode"] = MODES[o]
						zones["features"][index]["properties"]["ind"] = index
						zones["features"][index]["properties"]["income"] = ( str(inc * INCOME_SEP) if inc == 0 else str(inc * INCOME_SEP + 1) ) + "-" + str((inc+1) * INCOME_SEP)
						zones["features"][index]["properties"]["time"] = str(t) + ":00:00"
						index += 1
						if index % 1000 == 0:
							print ("Creating zones: " + str(index - len(polys)) + " / " + str(math.ceil(200000 / INCOME_SEP) * math.ceil(24 * 60 * 60 / TIME_SEP) * len(MODES) * len(polys)))
	else:
		for o in range(len(MODES)):
			for i in range(len(polys)+1):
				if o == 0 and i == 0:
					continue
				for j in range(len(polys)):
					poly = zones["features"][j]
					zones["features"].append(copy.deepcopy(poly))
					if i == 0:
						zones["features"][index]["properties"]["focusedNode"] = "all"
					else:
						zones["features"][index]["properties"]["focusedNode"] = zones["features"][i-1]["properties"]["name"]
					zones["features"][index]["properties"]["mode"] = MODES[o]
					zones["features"][index]["properties"]["ind"] = index
					# zones["features"][index]["properties"]["income"] = ( str(inc * INCOME_SEP) if inc == 0 else str(inc * INCOME_SEP + 1) ) + "-" + str((inc+1) * INCOME_SEP)
					# zones["features"][index]["properties"]["time"] = 1
					index += 1
					if index % 1000 == 0:
						print ("Creating zones: " + str(index - len(polys)) + " / " + str(len(polys) * len(polys) * len(MODES))) # * math.ceil(24 * 60 * 60 / TIME_SEP)))
	
	#Create links
	linkMap = [[] for i in range(100000)]
	for i in range(len(links["LinkId"])):
		linkMap[int(links["LinkId"][i])] = [int(links["fromLocationID"][i]), int(links["toLocationID"][i])]
	nodeMap = [[] for i in range(100000)]
	for i in range(len(nodes["NodeId"])):
		nodeMap[int(nodes["NodeId"][i])] = [nodes["x"][i], nodes["y"][i]]
	popMap = {}
	for i in range(len(population["PID"])):
		popMap[population["PID"][i]] = int(population["income"][i])
	# actMap = {} # PID + AcID --> FID
	# for i in range(len(acts["PID"])):
	# 	pid = acts["PID"][i]
	# 	if not (pid in list(actMap.keys())):
	# 		actMap[pid] = []
	# 	actMap[pid].append(acts["FID"][i])
	# facMap = {} #FID --> polyInd
	# for i in range(len(facilities["FID"])):
	# 	if i % 1000 == 0:
	# 		print ("Parsing facilities: " + str(i) + " / " + str(len(facilities["FID"])))
	# 	node = int(facilities["NodeID"][i])
	# 	pt = geo.Point(nodeMap[node])
	# 	for j, poly in enumerate(polys):
	# 		if pt.within(poly):
	# 			facMap[facilities["FID"][i]] = j
	# 			break

	legMap = {}
	ignored = 0
	for i in range(len(legs["PID"])):
		if i % 1000 == 0:
			print ("Parsing legs: " + str(i) + " / " + str(len(legs["PID"])))
		pid = legs["PID"][i]
		if not (pid in list(legMap.keys())):
			legMap[pid] = []
		leg_links = legs["LinkId"][i]
		st = 0
		end = 0
		try:
			if linkMap[int(float(leg_links[0]))][0] in linkMap[int(float(leg_links[1]))]:
				st = linkMap[int(float(leg_links[0]))][1]
			else:
				st = linkMap[int(float(leg_links[0]))][0]
			if linkMap[int(float(leg_links[-1]))][0] in linkMap[int(float(leg_links[-2]))]:
				end = linkMap[int(float(leg_links[-1]))][1]
			else:
				end = linkMap[int(float(leg_links[-1]))][0]
		except:
			ignored+=1
		stPt = geo.Point(nodeMap[st])
		endPt = geo.Point(nodeMap[end])
		stInd = 0
		endInd = 0
		for j, poly in enumerate(polys):
			if stPt.within(poly):
				stInd = j
			if endPt.within(poly):
				endInd = j
		if int(legs["Leg_ID"][i]) == 1:
			legMap[pid].append([stInd, 0])
		else:
			legMap[pid][int(legs["Trip_ID"][i])-1][1] = endInd

	print ("Legs parsed: " + str(len(legs["PID"]) - ignored))
	print ("Legs ignored: " + str(ignored))

	#Parse input per trip
	ignored = 0
	ignored_modes = []
	for i in range(len(trips["PID"])):
		# for i in range(1):
		if i % 1000 == 0:
			print("Parsing trips: " + str(i) + " / " + str(len(trips["PID"])))
		stInd = legMap[trips["PID"][i]][int(trips["Trip_num"][i])-1][0] #facMap[actMap[trips["PID"][i]][trips["OriginAct"][i]]]
		stInd = legMap[trips["PID"][i]][int(trips["Trip_num"][i])-1][1] #facMap[actMap[trips["PID"][i]][trips["DestinationAct"][i]]]
		try:
			modeFactor = MODES.index(trips["realizedTripMode"][i])
		except:
			ignored+=1
			if not (trips["realizedTripMode"][i] in ignored_modes):
				ignored_modes.append(trips["realizedTripMode"][i])
			continue
		
		inds = []
		if isTAZ:
			wage = popMap[trips["PID"][i]]
			time = (int(trips["End_time"][i]) + int(trips["Start_time"][i])) / 2
			wageFactor = max((wage-1) // INCOME_SEP, 0)
			timeFactor = max((time-1) // TIME_SEP, 0)
			ind = int(timeFactor * math.ceil(200000 / INCOME_SEP) * len(MODES) * len(polys) + wageFactor * len(MODES) * len(polys) + modeFactor * len(polys) + stInd)
			all_modeInd = int(timeFactor * math.ceil(200000 / INCOME_SEP) * len(MODES) * len(polys) + wageFactor * len(MODES) * len(polys) + stInd)
			inds = [ind, all_modeInd]
		else:
			ind = modeFactor * (len(polys)+1) * len(polys) + (endInd+1) * len(polys) + stInd
			all_modeInd = (endInd+1) * len(polys) + stInd
			all_fromInd = modeFactor * (len(polys)+1) * len(polys) + stInd
			all_allInd = stInd 
			inds = [ind, all_modeInd, all_fromInd, all_allInd]
		# print("Start: " + str(stInd) + ", End: " + str(endInd) + " --> " + str(ind))
		cost = (int(trips["fuelCost"][i]) + int(trips["Toll"][i])) if int(trips["Fare"][i]) == 0 else int(trips["Fare"][i])
		for o, index in enumerate(inds):
			zones["features"][index]["properties"]["totCost"] += cost
			zones["features"][index]["properties"]["numVals"] += 1

	print ("Trips parsed: " + str(len(legs["PID"]) - ignored))
	print ("Trips ignored: " + str(ignored))
	print ("Modes ignored: " + ", ".join(ignored_modes))
	
	for i, poly in enumerate(zones["features"]):
		if poly["properties"]["numVals"] > 0:
			poly["properties"]["avgCost"] = round(poly["properties"]["totCost"] / poly["properties"]["numVals"] * 10) / 10
	
	with open (OUTPUT_FOLDER+"/costsByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "w") as out:
		json.dump(zones, out, allow_nan=True)
def modeShareByZone(isTAZ):
	global trips, links, population, legs, nodes
	
	#Run as multithread
	def processNodes (**kwargs):
		'''kwargs: polys, nodes, st, end'''
		nodes = kwargs.get("nodes")
		polys = kwargs.get("polys")
		st = kwargs.get("st")
		end = kwargs.get("end")

		nMap = [0 for i in range(100000)]
		for i in range(st, end):
			# nodeMap[int(n["properties"]["id"])] = n["geometry"]["coordinates"]
			pos = geo.Point([nodes["x"][i], nodes["y"][i]])
			for i, poly in enumerate(polys):
				if pos.within(poly):
					nMap[int(nodes["NodeId"][i])] = i
					break
		return nMap
	def processLegs (**kwargs):
		'''kwargs: legs, nodeMap, linkMap, stL, endL'''
		stL = kwargs.get("stL")
		endL = kwargs.get("endL")
		legs = kwargs.get("legs")
		nodeMap = kwargs.get("nodeMap")
		linkMap = kwargs.get("linkMap")
		legMap = {}
		ignored = 0
		for i in range(stL, endL):
			try: # It seems like pandas dataframe (maybe numpy array?) don't support multi-threaded processes
				pid = legs["PID"][i]
				leg_links = legs["LinkId"][i]
				legID = legs["Leg_ID"][i]
			except:
				ignored += 1
				continue
			if not (pid in list(legMap.keys())):
				legMap[pid] = []
			st = 0
			end = 0
			try:
				if linkMap[int(float(leg_links[0]))][0] in linkMap[int(float(leg_links[1]))]:
					st = linkMap[int(float(leg_links[0]))][1]
				else:
					st = linkMap[int(float(leg_links[0]))][0]
				if linkMap[int(float(leg_links[-1]))][0] in linkMap[int(float(leg_links[-2]))]:
					end = linkMap[int(float(leg_links[-1]))][1]
				else:
					end = linkMap[int(float(leg_links[-1]))][0]
			except:
				ignored+=1
			stInd = nodeMap[st]
			endInd = nodeMap[end]
			if int(legID) == 1:
				legMap[pid].append([stInd, 0])
			else:
				legMap[pid].append([stInd, endInd])
		return [legMap, ignored]
	def processTrips (**kwargs):
		'''kwargs: trips, popMap, legMap, stP, endP'''
		trips = kwargs.get("trips")
		popMap = kwargs.get("popMap")
		legMap = kwargs.get("legMap")
		stP = kwargs.get("stP")
		endP = kwargs.get("endP")
		missingNum = 0
		ignored = 0
		ignored_modes = []

		for i in range(stP, endP):
		# for i in range(1):
			# if i % 1000 == 0:
			# 	print("Parsing trips: " + str(i) + " / " + str(len(trips["PID"])))
			stInd = 0
			endInd = 0
			pid = tid = stTime = endTime = mode = ""
			try: # It seems like pandas dataframe (maybe numpy array?) don't support multi-threaded processes
				pid = trips["PID"][i]
				tid = trips["Trip_ID"][i]
				stTime = trips["Start_time"][i]
				endTime = trips["End_time"][i]
				mode = trips["realizedTripMode"][i]
			except:
				ignored += 1
				continue
			try:
				stInd = int(legMap[str(pid)][int(tid)-1][0]) #facMap[actMap[trips["PID"][i]][trips["OriginAct"][i]]]
				endInd = int(legMap[str(pid)][int(tid)-1][1]) #facMap[actMap[trips["PID"][i]][trips["DestinationAct"][i]]]
			except:
				threadLock.acquire()
				with open("../output_files/missingPIDs.csv", "a") as missing:
					missing.write(pid + ",")
				threadLock.release()
				missingNum += 1
				ignored += 1
				continue
			
			if not (mode in MODES):
				ignored+=1
				if not (mode in ignored_modes):
					ignored_modes.append(mode)
				continue
		
			inds = []
			if isTAZ:
				wage = popMap[pid]
				wageFactor = max((wage-1) // INCOME_SEP, 0)
				time = min((int(endTime) + int(stTime)) / 2, 24*60*60) # Weird stuff, sometimes endTime > 24
				timeFactor = int(max((time-1) // TIME_SEP, 0))
				for j in range(len(MODE_GROUPS)):
					inds.append(int(j * math.ceil(24 * 60 * 60 / TIME_SEP) * math.ceil(200000 / INCOME_SEP) * len(polys) + timeFactor * math.ceil(200000 / INCOME_SEP) * len(polys) + wageFactor * len(polys) + stInd))
			else:
				for j in range(len(MODE_GROUPS)):
					inds.append(int(j * (len(polys)+1) * len(polys) + (endInd+1) * len(polys) + stInd))
					inds.append(int(j * (len(polys)+1) * len(polys) + stInd))
		# print("Start: " + str(stInd) + ", End: " + str(endInd) + " --> " + str(ind))
			threadLock.acquire()
			for index in inds:
				zones["features"][index]["properties"][mode] += 1
				if str(zones["features"][index]["properties"]["modal_group"]) == MODE_GROUPS[MODE_GROUP_DICT[mode]]:
					zones["features"][index]["properties"]["modal_count"] += 1
			threadLock.release()
		return [missingNum, ignored, ignored_modes]

	if os.path.exists("../output_files/missingPIDs.csv"):
		os.remove("../output_files/missingPIDs.csv")

	print ("Creating visual: modeShareByZone. IsTAZ = " + str(isTAZ))
	zones = {}
	print ("Copying zone information..")
	if isTAZ:
		zones = copy.deepcopy(TAZZones)
	else:
		zones = copy.deepcopy(neighborZones)
	polys = []
	for i, poly in enumerate(zones["features"]):
		# print(poly["geometry"]["coordinates"][0])
		polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
		for j, m in enumerate(MODES):
			poly["properties"][m] = 0
			poly["properties"][m + "_percentage"] = ""
		poly["properties"]["modal_group"] = MODE_GROUPS[0]
		poly["properties"]["modal_count"] = 0
		poly["properties"]["modal_percentage"] = 0
		poly["properties"]["modal_percentage_hidden"] = 0
		poly["properties"]["ind"] = i
		if isTAZ:
			poly["properties"]["income"] = "0-" + str(INCOME_SEP)
			poly["properties"]["time"] = "0:00:00"
		else:
			poly["properties"]["focusedNode"] = "all"

	index = len(zones["features"])
	if isTAZ:
		for M in range(len(MODE_GROUPS)):
			for t in range(math.ceil(24 * 60 * 60 / TIME_SEP)):
				for inc in range(math.ceil(200000 / INCOME_SEP)):
					if inc == 0 and M == 0 and t == 0:
						continue
					for j in range(len(polys)):
						poly = zones["features"][j]
						zones["features"].append(copy.deepcopy(poly))
						zones["features"][index]["properties"]["modal_group"] = MODE_GROUPS[M]
						zones["features"][index]["properties"]["ind"] = index
						zones["features"][index]["properties"]["time"] = "%d:%02d:%02d" %((t * TIME_SEP) // 3600, ((t*TIME_SEP) // 60) % 60, (t * TIME_SEP) % 60)
						zones["features"][index]["properties"]["income"] = ( str(inc * INCOME_SEP) if inc == 0 else str(inc * INCOME_SEP + 1) ) + "-" + str((inc+1) * INCOME_SEP)
						index += 1
						if index % 1000 == 0:
							print ("Creating zones: " + str(index - len(polys)) + " / " + str(math.ceil(200000 / INCOME_SEP) * math.ceil(24 * 60 * 60 / TIME_SEP) * len(polys) * len(MODE_GROUPS)))
	else:
		for M in range(len(MODE_GROUPS)):
			for i in range(len(polys)+1):
				if i == 0 and M == 0:
					continue
				for j in range(len(polys)):
					poly = zones["features"][j]
					zones["features"].append(copy.deepcopy(poly))
					if i == 0:
						zones["features"][index]["properties"]["focusedNode"] = "all"
					else:
						zones["features"][index]["properties"]["focusedNode"] = zones["features"][i-1]["properties"]["name"]
					zones["features"][index]["properties"]["modal_group"] = MODE_GROUPS[M]
					zones["features"][index]["properties"]["ind"] = index
					index += 1
					if index % 1000 == 0:
						print ("Creating zones: " + str(index - len(polys)) + " / " + str((len(polys)+1) * len(polys) * len(MODE_GROUPS)))
	#Create links
	linkMap = [[] for i in range(100000)]
	for i in range(len(links["LinkId"])):
		linkMap[int(links["LinkId"][i])] = [int(links["fromLocationID"][i]), int(links["toLocationID"][i])]
	
	nodeMap = [0 for i in range(100000)]
	threads = []
	for i in range(NUM_THREADS):
		st = (i * len(nodes["NodeId"])) // NUM_THREADS
		end = ((i+1) * len(nodes["NodeId"])) // NUM_THREADS
		threads.append(processingThread(i, "node_thread_" + str(i), processNodes, polys=polys, nodes=nodes, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		for i, num in enumerate(thread.out):
			if num > 0:
				nodeMap[i] = num
		
	popMap = {}
	for i in range(len(population["PID"])):
		popMap[population["PID"][i]] = int(population["income"][i])
	
	legMap = {}
	ignored = 0
	threads = []
	for i in range(NUM_THREADS):
		st = (i * len(legs["PID"])) // NUM_THREADS
		end = ((i+1) * len(legs["PID"])) // NUM_THREADS
		threads.append(processingThread(i, "leg_thread_" + str(i), processLegs, legs=legs, nodeMap=nodeMap, linkMap=linkMap, stL=st, endL=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		for legKey in thread.out[0].keys():
			if legKey in legMap:
				legMap[legKey] = legMap[legKey] + thread.out[0][legKey]
			else:
				legMap[legKey] = thread.out[0][legKey]
		ignored += thread.out[1]

	print ("Legs parsed: " + str(len(legs["PID"]) - ignored))
	print ("Legs ignored: " + str(ignored))

	with open("../output_files/legs.json", "w") as legOut:
		legOut.write(json.dumps(legMap))
	
	#Parse input per trip
	
	ignored = 0
	ignored_modes = []
	missingNum = 0
	threadLock = threading.Lock()
	threads = []
	for i in range(NUM_THREADS):
		st = (i * len(trips["PID"])) // NUM_THREADS
		end = ((i+1) * len(trips["PID"])) // NUM_THREADS
		threads.append(processingThread(i, "trip_thread_" + str(i), processTrips, trips=trips, popMap=popMap, legMap=legMap, stP=st, endP=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		missingNum += thread.out[0]
		ignored += thread.out[1]
		for i, m in enumerate(thread.out[2]):
			if not (m in ignored_modes):
				ignored_modes.append(m)

	print ("Trips parsed: " + str(len(trips["PID"]) - ignored))
	print ("Trips ignored / broken: " + str(ignored))
	print ("Modes ignored: " + ", ".join(ignored_modes))
	print ("%i PIDs missing from the legs table. See ../output_files/missingPIDs.csv for the full list" %(missingNum))

	for i, poly in enumerate(zones["features"]):
		totNum = 0
		for j, m in enumerate(MODES):
			totNum += poly["properties"][m]
		if totNum == 0:
			continue
		for j, m in enumerate(MODES):
			poly["properties"][m + "_percentage"] = "%i%% "%((poly["properties"][m] / totNum) * 100)
		poly["properties"]["modal_percentage"] = "%i%%"%((poly["properties"]["modal_count"] / totNum) * 100)
		poly["properties"]["modal_percentage_hidden"] = (poly["properties"]["modal_count"] / totNum)
			
	with open (OUTPUT_FOLDER+"/costsByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "w") as out:
		json.dump(zones, out, allow_nan=True)
def speedByZone(isTAZ):
	global links, speeds, nodes
	def processNodes (**kwargs):
		'''kwargs: polys, nodes, st, end'''
		nodes = kwargs.get("nodes")
		polys = kwargs.get("polys")
		st = kwargs.get("st")
		end = kwargs.get("end")

		nMap = [0 for i in range(100000)]
		for i in range(st, end):
			# nodeMap[int(n["properties"]["id"])] = n["geometry"]["coordinates"]
			pos = geo.Point([nodes["x"][i], nodes["y"][i]])
			for i, poly in enumerate(polys):
				if pos.within(poly):
					nMap[int(nodes["NodeId"][i])] = i
					break
		return nMap
	def processSpeeds (**kwargs):
		'''kwargs: speeds, linkMap, polys, stP, endP'''
		speeds = kwargs.get("speeds")
		polys = kwargs.get("polys")
		stP = kwargs.get("stP")
		endP = kwargs.get("endP")

		for i in range(stP, endP):
			inds = []
			line = speeds[i].split(",")
			speed = float(line[11])
			fromNode = int(line[2])
			toNode = int(line[3])
			hour = float(line[4])
			length = float(line[5])
			fromInd = int(nodeMap[fromNode])
			toInd = int(nodeMap[toNode])
			linkFactor = 0 if length > 50 else 1
			if not isTAZ:
				timeFactor = int(max((hour * 3600 - 1) // TIME_SEP, 0))
				inds.append(timeFactor * 2 * len(polys) + linkFactor * len(polys) + fromInd)
				inds.append(timeFactor * 2 * len(polys) + linkFactor * len(polys) + toInd)
			else:
				inds.append(linkFactor * len(polys) + fromInd)
				inds.append(linkFactor * len(polys) + toInd)
			# print("Start: " + str(stInd) + ", End: " + str(endInd) + " --> " + str(ind))
			threadLock.acquire()
			for index in inds:
				zones["features"][index]["properties"]["totSpeed"] += speed
				zones["features"][index]["properties"]["numVals"] += 1
			threadLock.release()
		return None

		print ("Creating visual: costsByZone. IsTAZ = " + str(isTAZ))

	print ("Creating visual: speedByZone. IsTAZ = " + str(isTAZ))

	zones = {}
	print ("Copying zone information..")
	if isTAZ:
		zones = copy.deepcopy(TAZZones)
	else:
		zones = copy.deepcopy(neighborZones)
	polys = []
	for i, poly in enumerate(zones["features"]):
		# print(poly["geometry"]["coordinates"][0])
		polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
		poly["properties"]["totSpeed"] = 0
		poly["properties"]["numVals"] = 0
		poly["properties"]["avgSpeed"] = 0
		poly["properties"]["ind"] = i
		poly["properties"]["linkType"] = "motorway"
		if not isTAZ:
			poly["properties"]["time"] = "0:00:00"

	index = len(polys)
	if not isTAZ:
		for t in range(math.ceil(24 * 60 * 60 / TIME_SEP)):
			for j in range(2):
				for i in range(len(polys)):
					if t == 0 and j == 0:
						continue
					poly = zones["features"][i]
					zones["features"].append(copy.deepcopy(poly))
					zones["features"][index]["properties"]["linkType"] = "motorway" if i == 0 else "residential"
					zones["features"][index]["properties"]["ind"] = index
					zones["features"][index]["properties"]["time"] = "%d:%02d:%02d" %((t * TIME_SEP) // 3600, ((t*TIME_SEP) // 60) % 60, (t * TIME_SEP) % 60)
					index += 1
	else:
		for i in range(len(polys)):
			poly = zones["features"][i]
			zones["features"].append(copy.deepcopy(poly))
			zones["features"][index]["properties"]["linkType"] = "residential"
			zones["features"][index]["properties"]["ind"] = index
			index += 1
	nodeMap = [0 for i in range(100000)]
	threads = []
	for i in range(NUM_THREADS):
		st = (i * len(nodes["NodeId"])) // NUM_THREADS
		end = ((i+1) * len(nodes["NodeId"])) // NUM_THREADS
		threads.append(processingThread(i, "node_thread_" + str(i), processNodes, polys=polys, nodes=nodes, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		for i, num in enumerate(thread.out):
			if num > 0:
				nodeMap[i] = num
	
	threadLock = threading.Lock()
	threads = []
	for i in range(NUM_THREADS):
		st = max((i * len(speeds)) // NUM_THREADS, 1)
		end = ((i+1) * len(speeds)) // NUM_THREADS
		threads.append(processingThread(i, "data_thread_" + str(i), processSpeeds, speeds=speeds, polys=polys, stP=st, endP=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
	
	for i, zone in enumerate(zones["features"]):
		zone["properties"]["avgSpeed"] = zone["properties"]["totSpeed"] / zone["properties"]["numVals"]
	with open (OUTPUT_FOLDER+"/speedByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "w") as out:
		json.dump(zones, out, allow_nan=True)
def VMTByZone (isTAZ):
	global links, speeds, nodes
	def processNodes (**kwargs):
		'''kwargs: polys, nodes, st, end'''
		nodes = kwargs.get("nodes")
		polys = kwargs.get("polys")
		st = kwargs.get("st")
		end = kwargs.get("end")

		nMap = [0 for i in range(100000)]
		for i in range(st, end):
			# nodeMap[int(n["properties"]["id"])] = n["geometry"]["coordinates"]
			pos = geo.Point([nodes["x"][i], nodes["y"][i]])
			for i, poly in enumerate(polys):
				if pos.within(poly):
					nMap[int(nodes["NodeId"][i])] = i
					break
		return nMap
	def processVMT (**kwargs):
		'''kwargs: data, linkMap, polys, stP, endP'''
		data = kwargs.get("data")
		polys = kwargs.get("polys")
		stP = kwargs.get("stP")
		endP = kwargs.get("endP")

		for i in range(stP, endP):
			inds = []
			line = data[i].split(",")
			volume = float(line[9])
			fromNode = int(line[2])
			toNode = int(line[3])
			hour = float(line[4])
			length = float(line[5])
			fromInd = int(nodeMap[fromNode])
			toInd = int(nodeMap[toNode])
			linkFactor = 0 if length > 50 else 1
			meterToMile = 0.0006213712
			if not isTAZ:
				timeFactor = int(max((hour * 3600 - 1) // TIME_SEP, 0))
				inds.append(timeFactor * 2 * len(polys) + linkFactor * len(polys) + fromInd)
				inds.append(timeFactor * 2 * len(polys) + linkFactor * len(polys) + toInd)
			else:
				inds.append(linkFactor * len(polys) + fromInd)
				inds.append(linkFactor * len(polys) + toInd)
			# print("Start: " + str(stInd) + ", End: " + str(endInd) + " --> " + str(ind))
			threadLock.acquire()
			for index in inds:
				zones["features"][index]["properties"]["VMT"] += volume * length * meterToMile
			threadLock.release()
		return None

	print ("Creating visual: VMTByZone. IsTAZ = " + str(isTAZ))
	zones = {}
	print ("Copying zone information..")
	if isTAZ:
		zones = copy.deepcopy(TAZZones)
	else:
		zones = copy.deepcopy(neighborZones)
	polys = []
	for i, poly in enumerate(zones["features"]):
		# print(poly["geometry"]["coordinates"][0])
		polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
		poly["properties"]["VMT"] = 0
		poly["properties"]["ind"] = i
		poly["properties"]["linkType"] = "motorway"
		if not isTAZ:
			poly["properties"]["time"] = "0:00:00"
			poly["properties"]["mode"] = MODES[0]
			
	
	index = len(polys)
	if not isTAZ: # + per mode
		for m in range(len(MODES)):
			for i in range(2):
				for t in range(math.ceil(24 * 60 * 60 / TIME_SEP)):
					if m == 0 and i == 0 and t == 0:
						continue
					for j in range(len(polys)):
						poly = zones["features"][j]
						zones["features"].append(copy.deepcopy(poly))
						zones["features"][index]["properties"]["ind"] = index
						zones["features"][index]["properties"]["time"] = "%d:%02d:%02d" %((t * TIME_SEP) // 3600, ((t*TIME_SEP) // 60) % 60, (t * TIME_SEP) % 60)
						zones["features"][index]["properties"]["linkType"] = ["motorway", "residential"][i]
						zones["features"][index]["properties"]["mode"] = MODES[m]
						index += 1
						if index % 1000 == 0:
							print ("Creating zones: " + str(index - len(polys)) + " / " + str(len(MODES) * 2 * math.ceil(24 * 60 * 60 / TIME_SEP)))
	else:
		for j in range(len(polys)):
			poly = zones["features"][j]
			zones["features"].append(copy.deepcopy(poly))
			zones["features"][index]["properties"]["ind"] = index
			zones["features"][index]["properties"]["linkType"] = "residential"
			index += 1
			if index % 1000 == 0:
				print ("Creating zones: " + str(index - len(polys)) + " / " + str(len(polys)))
	nodeMap = [0 for i in range(100000)]
	threads = []
	for i in range(NUM_THREADS):
		st = (i * len(nodes["NodeId"])) // NUM_THREADS
		end = ((i+1) * len(nodes["NodeId"])) // NUM_THREADS
		threads.append(processingThread(i, "node_thread_" + str(i), processNodes, polys=polys, nodes=nodes, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		for i, num in enumerate(thread.out):
			if num > 0:
				nodeMap[i] = num
	
	threadLock = threading.Lock()
	threads = []
	for i in range(NUM_THREADS):
		st = max((i * len(speeds)) // NUM_THREADS, 1)
		end = ((i+1) * len(speeds)) // NUM_THREADS
		threads.append(processingThread(i, "data_thread_" + str(i), processVMT, data=speeds, polys=polys, stP=st, endP=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()

	with open (OUTPUT_FOLDER+"/VMT"+("TAZ" if isTAZ else "neighbors")+".json", "w") as out:
		json.dump(zones, out, allow_nan=True)
def occupancyByZone(isTAZ):
	global paths, links, vehicles, vehicleTypes, links, nodes
	def processNodes (**kwargs):
		'''kwargs: polys, nodes, st, end'''
		nodes = kwargs.get("nodes")
		polys = kwargs.get("polys")
		st = kwargs.get("st")
		end = kwargs.get("end")

		nMap = [0 for i in range(100000)]
		for i in range(st, end):
			# nodeMap[int(n["properties"]["id"])] = n["geometry"]["coordinates"]
			pos = geo.Point([nodes["x"][i], nodes["y"][i]])
			for i, poly in enumerate(polys):
				if pos.within(poly):
					nMap[int(nodes["NodeId"][i])] = i
					break
		return nMap
	def processPaths (**kwargs):
		'''kwargs: paths, linkMap, st, end'''
		paths = kwargs.get("paths")
		linkMap = kwargs.get("linkMap")
		st = kwargs.get("st")
		end = kwargs.get("end")
		out = []
		for i in range(st, end):
			threadLock.acquire()
			try:
				path = list(map(lambda x: int(x), paths["path"][i]))
			except:
				out.append([None])
				threadLock.release()
				continue
			threadLock.release()
			if len(path) < 2:
				out.append([None])
				continue
			origin = linkMap[path[0]]
			if origin[0] == linkMap[path[1]][1] or origin[0] == linkMap[path[1]][1]:
				out.append([origin[1]])
			else:
				out.append([origin[0]])
			for link in path:
				stL, endL = linkMap[int(link)]
				if stL == out[-1][-1]:
					out[-1].append(endL)
				else:
					out[-1].append(stL)
		return out
	def processVehicles (**kwargs):
		'''kwargs: vehicles, st, end'''
		vehicles = kwargs.get("vehicles")
		st = kwargs.get("st")
		end = kwargs.get("end")
		out = {}
		for i in range(st, end):
			threadLock.acquire()
			out[str(vehicles["vehicle"][i])] = str(vehicles["vehicleType"][i])
			threadLock.release()
		return out
	def processVehicleTypes (**kwargs):
		'''kwargs: vehicleTypes, st, end'''
		vehicleTypes = kwargs.get("vehicleTypes")
		st = kwargs.get("st")
		end = kwargs.get("end")
		out = {}
		for i in range(st, end):
			vehicleType = vehicleTypes["vehicleTypeId"][i]
			capacity = int(vehicleTypes["seatingCapacity"][i]) + int(vehicleTypes["standingRoomCapacity"][i])
			out[vehicleType] = capacity
		return out
	def processData (**kwargs):
		'''kwargs: paths, pathList, nodeMap, vehicleMap, vehicleTypeMap, st, end'''
		paths = kwargs.get("paths")
		pathList = kwargs.get("pathList")
		nodeMap = kwargs.get("nodeMap")
		vehicleMap = kwargs.get("vehicleMap")
		vehicleTypeMap = kwargs.get("vehicleTypeMap")
		st = kwargs.get("st")
		end = kwargs.get("end")
		
		for i in range(st, end):
			threadLock.acquire()
			stTime = paths["departureTime"][i]
			endTime = paths["arrivalTime"][i]
			occupancy = paths["numPassengers"][i]
			vehicleID = paths["vehicle"][i]
			mode = paths["mode"][i]
			vehicleType = vehicleMap[vehicleID]
			capacity = vehicleTypeMap[vehicleType]
			threadLock.release()
			for node in pathList[i]:
				if not node:
					continue
				zone = nodeMap[node]
				time = min((int(endTime) + int(stTime)) / 2, 24*60*60) # The viz is made for a 24h simulation, but it's actually a 30 hour simulation because that makes sense
				timeFactor = int(max((time-1) // TIME_SEP, 0))
				modeFactor = MODES.index(mode)

				threadLock.acquire()
				if isTAZ:
					ind = zone

					zones["features"][zone]["properties"]["totalVehicleOccupancy"] += occupancy / capacity
					zones["features"][ind]["properties"]["numVals"] += 1
				else:
					ind = modeFactor * math.ceil(24 * 60 * 60 / TIME_SEP) * len(polys) + timeFactor * len(polys) + zone
					zones["features"][ind]["properties"]["totalVehicleOccupancy"] += occupancy / capacity
					zones["features"][ind]["properties"]["numVals"] += 1
				threadLock.release()

	print ("Creating visual: occupancyByZone. IsTAZ = " + str(isTAZ))
	zones = {}
	print ("Copying zone information..")
	if isTAZ:
		zones = copy.deepcopy(TAZZones)
	else:
		zones = copy.deepcopy(neighborZones)
	polys = []
	for i, poly in enumerate(zones["features"]):
		# print(poly["geometry"]["coordinates"][0])
		polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
		poly["properties"]["totalVehicleOccupancy"] = 0
		poly["properties"]["numVals"] = 0
		poly["properties"]["avgVehicleOccupancy"] = None
		poly["properties"]["ind"] = i
		if not isTAZ:
			poly["properties"]["time"] = "0:00:00"
			poly["properties"]["mode"] = MODES[0]

	index = len(zones["features"])
	if not isTAZ:
		for m in range(len(MODES)):
			for t in range(math.ceil(24 * 60 * 60 / TIME_SEP)):
				if m == 0 and t == 0:
					continue
				for j in range(len(polys)):
					poly = zones["features"][j]
					zones["features"].append(copy.deepcopy(poly))
					zones["features"][index]["properties"]["time"] = "%d:%02d:%02d" %((t * TIME_SEP) // 3600, ((t*TIME_SEP) // 60) % 60, (t * TIME_SEP) % 60)
					zones["features"][index]["properties"]["mode"] = MODES[m]
					zones["features"][index]["properties"]["ind"] = index
					index += 1
					if index % 1000 == 0:
						print ("Creating zones: " + str(index) + " / " + str(len(polys) * math.ceil(24 * 60 * 60 / TIME_SEP) * len(MODES)))

	nodeMap = [0 for i in range(100000)]
	threads = []
	for i in range(NUM_THREADS):
		st = (i * len(nodes["NodeId"])) // NUM_THREADS
		end = ((i+1) * len(nodes["NodeId"])) // NUM_THREADS
		threads.append(processingThread(i, "node_thread_" + str(i), processNodes, polys=polys, nodes=nodes, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		for i, num in enumerate(thread.out):
			if num > 0:
				nodeMap[i] = num

	linkMap = [0 for i in range(100000)]
	for i in range(len(links["LinkId"])):
		linkMap[int(links["LinkId"][i])] = (links["fromLocationID"][i], links["toLocationID"][i])

	threadLock = threading.Lock()
	pathList = []
	threads = []
	for i in range(NUM_THREADS):
		st = (i * len(paths["path"])) // NUM_THREADS
		end = ((i+1) * len(paths["path"])) // NUM_THREADS
		threads.append(processingThread(i, "path_thread_" + str(i), processPaths, paths=paths, linkMap=linkMap, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		pathList = pathList + thread.out
	
	vehicleMap = {}
	threads = []
	for i in range(NUM_THREADS):
		st = (i * len(vehicles["vehicle"])) // NUM_THREADS
		end = ((i+1) * len(vehicles["vehicle"])) // NUM_THREADS
		threads.append(processingThread(i, "vehicle_thread_" + str(i), processVehicles, vehicles=vehicles, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		vehicleMap.update(thread.out)

	vehicleTypeMap = {}
	threads = []
	for i in range(NUM_THREADS):
		st = (i * len(vehicleTypes["vehicleTypeId"])) // NUM_THREADS
		end = ((i+1) * len(vehicleTypes["vehicleTypeId"])) // NUM_THREADS
		threads.append(processingThread(i, "vehicleType_thread_" + str(i), processVehicleTypes, vehicleTypes=vehicleTypes, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		vehicleTypeMap.update(thread.out)

	threads = []
	threadLock = threading.Lock()
	for i in range(NUM_THREADS):
		st = (i * len(paths["path"])) // NUM_THREADS
		end = ((i+1) * len(paths["path"])) // NUM_THREADS
		threads.append(processingThread(i, "data_thread_" + str(i), processData, paths=paths, pathList=pathList, nodeMap=nodeMap, vehicleMap=vehicleMap, vehicleTypeMap=vehicleTypeMap, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
	
	with open (OUTPUT_FOLDER+"/occupancyByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "w") as out:
		json.dump(zones, out, allow_nan=True)
def timeDelayByZone(isTAZ):
	global paths, population, links, nodes
	def processNodes (**kwargs):
		'''kwargs: polys, nodes, st, end'''
		nodes = kwargs.get("nodes")
		polys = kwargs.get("polys")
		st = kwargs.get("st")
		end = kwargs.get("end")

		nMap = [0 for i in range(100000)]
		for i in range(st, end):
			# nodeMap[int(n["properties"]["id"])] = n["geometry"]["coordinates"]
			pos = geo.Point([nodes["x"][i], nodes["y"][i]])
			for i, poly in enumerate(polys):
				if pos.within(poly):
					nMap[int(nodes["NodeId"][i])] = i
					break
		return nMap
	def processPaths (**kwargs):
		'''kwargs: paths, linkMap, st, end'''
		paths = kwargs.get("paths")
		linkMap = kwargs.get("linkMap")
		st = kwargs.get("st")
		end = kwargs.get("end")
		out = []
		for i in range(st, end):
			threadLock.acquire()
			try:
				path = list(map(lambda x: int(x), paths["path"][i]))
			except:
				out.append([None])
				threadLock.release()
				continue
			threadLock.release()
			if len(path) < 2:
				out.append([None])
				continue
			origin = linkMap[path[0]]
			if origin[0] == linkMap[path[1]][1] or origin[0] == linkMap[path[1]][1]:
				out.append([origin[1]])
			else:
				out.append([origin[0]])
			for link in path:
				stL = linkMap[int(link)][0]
				endL = linkMap[int(link)][1]
				if stL == out[-1][-1]:
					out[-1].append(endL)
				else:
					out[-1].append(stL)
		return out
	def processData (**kwargs):
		'''kwargs: paths, pathList, linkMap, popMap, nodeMap, st, end'''
		paths = kwargs.get("paths")
		pathList = kwargs.get("pathList")
		nodeMap = kwargs.get("nodeMap")
		popMap = kwargs.get("popMap")
		st = kwargs.get("st")
		end = kwargs.get("end")
		
		for i in range(st, end):
			threadLock.acquire()
			stTime = paths["departureTime"][i]
			endTime = paths["arrivalTime"][i]
			mode = paths["mode"][i]
			path = paths["path"][i]
			# passengers = paths["vehicleType0"][i]
			# for passenger in passengers:
				# income = popMap[passenger]
			threadLock.release()
			if not pathList[i][0]:
				continue
			origin = nodeMap[pathList[i][0]]
			destination = nodeMap[pathList[i][-1]]
			expectedTime = 0
			for link in path:
				expectedTime += linkMap[int(link)][3] / linkMap[int(link)][2]
			totalTime = endTime - stTime

			time = min((int(endTime) + int(stTime)) / 2, 24*60*60) # The viz is made for a 24h simulation, but it's actually a 30 hour simulation because that makes sense
			timeFactor = int(max((time-1) // TIME_SEP, 0))
			modeFactor = MODES.index(mode)
			incomeFactor = 0  #max((income-1) // INCOME_SEP, 0)
			inds = []
			if isTAZ:
				inds.append(modeFactor * math.ceil(24 * 60 * 60 / TIME_SEP) * math.ceil(200000 / INCOME_SEP) * len(polys) + timeFactor * math.ceil(200000 / INCOME_SEP) * len(polys) + incomeFactor * len(polys) + origin)
				inds.append(modeFactor * math.ceil(24 * 60 * 60 / TIME_SEP) * math.ceil(200000 / INCOME_SEP) * len(polys) + timeFactor * math.ceil(200000 / INCOME_SEP) * len(polys) + incomeFactor * len(polys) + destination)
			else:
				inds.append(modeFactor * (len(polys)+1) * len(polys) + origin * len(polys) + origin)
				inds.append(modeFactor * (len(polys)+1) * len(polys) + origin * len(polys) + destination)

			threadLock.acquire()
			for ind in inds:
				zones["features"][ind]["properties"]["timeDelay"] = totalTime - expectedTime
			threadLock.release()

	print ("Creating visual: timeDelayByZone. IsTAZ = " + str(isTAZ))
	zones = {}
	print ("Copying zone information..")
	if isTAZ:
		zones = copy.deepcopy(TAZZones)
	else:
		zones = copy.deepcopy(neighborZones)
	polys = []
	for i, poly in enumerate(zones["features"]):
		# print(poly["geometry"]["coordinates"][0])
		polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
		poly["properties"]["timeDelay"] = None
		poly["properties"]["mode"] = MODES[0]
		poly["properties"]["ind"] = i
		if isTAZ:
			poly["properties"]["time"] = "0:00:00"
			poly["properties"]["income"] = "0-" + str(INCOME_SEP)
		else:
			poly["properties"]["focusedNode"] = "all"

	index = len(zones["features"])
	if isTAZ:
		for m in range(len(MODES)):
			for t in range(math.ceil(24 * 60 * 60 / TIME_SEP)):
				for inc in range(math.ceil(200000 / INCOME_SEP)):
					if m == 0 and t == 0 and inc == 0:
						continue
					for j in range(len(polys)):
						poly = zones["features"][j]
						zones["features"].append(copy.deepcopy(poly))
						zones["features"][index]["properties"]["time"] = "%d:%02d:%02d" %((t * TIME_SEP) // 3600, ((t*TIME_SEP) // 60) % 60, (t * TIME_SEP) % 60)
						zones["features"][index]["properties"]["mode"] = MODES[m]
						zones["features"][index]["properties"]["income"] = ( str(inc * INCOME_SEP) if inc == 0 else str(inc * INCOME_SEP + 1) ) + "-" + str((inc+1) * INCOME_SEP)
						zones["features"][index]["properties"]["ind"] = index
						index += 1
						if index % 1000 == 0:
							print ("Creating zones: " + str(index) + " / " + str(len(polys) * math.ceil(24 * 60 * 60 / TIME_SEP) * len(MODES) * math.ceil(200000 / INCOME_SEP)))
	else:
		for m in range(len(MODES)):
			for i in range(len(polys)+1):
				if m == 0 and i == 0:
					continue
				for j in range(len(polys)+1):
					poly = zones["features"][j]
					zones["features"].append(copy.deepcopy(poly))
					zones["features"][index]["properties"]["mode"] = MODES[m]
					if i == 0:
						zones["features"][index]["properties"]["focusedNode"] = "all"
					else:
						zones["features"][index]["properties"]["focusedNode"] = zones["features"][i-1]["properties"]["name"]
					zones["features"][index]["properties"]["ind"] = index
					index += 1
					if index % 1000 == 0:
						print ("Creating zones: " + str(index) + " / " + str(len(polys) * len(MODES) * len(polys)+1))

	nodeMap = [0 for i in range(100000)]
	threads = []
	for i in range(NUM_THREADS):
		st = (i * len(nodes["NodeId"])) // NUM_THREADS
		end = ((i+1) * len(nodes["NodeId"])) // NUM_THREADS
		threads.append(processingThread(i, "node_thread_" + str(i), processNodes, polys=polys, nodes=nodes, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		for i, num in enumerate(thread.out):
			if num > 0:
				nodeMap[i] = num

	popMap = {}
	for i in range(len(population["PID"])):
		popMap[population["PID"][i]] = int(population["income"][i])

	linkMap = [0 for i in range(100000)]
	for i in range(len(links["LinkId"])):
		linkMap[int(links["LinkId"][i])] = (links["fromLocationID"][i], links["toLocationID"][i], links["freeSpeed"][i], links["length"][i])

	threadLock = threading.Lock()
	pathList = []
	threads = []
	for i in range(NUM_THREADS):
		st = (i * len(paths["path"])) // NUM_THREADS
		end = ((i+1) * len(paths["path"])) // NUM_THREADS
		threads.append(processingThread(i, "path_thread_" + str(i), processPaths, paths=paths, linkMap=linkMap, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		pathList = pathList + thread.out

	threads = []
	for i in range(NUM_THREADS):
		st = (i * len(paths["path"])) // NUM_THREADS
		end = ((i+1) * len(paths["path"])) // NUM_THREADS
		threads.append(processingThread(i, "processing_thread_" + str(i), processData, paths=paths, linkMap=linkMap, popMap=popMap, pathList=pathList, nodeMap=nodeMap, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
	
	with open (OUTPUT_FOLDER+"/timeDelayByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "w") as out:
		json.dump(zones, out, allow_nan=True)
def tripDensityByZone(isTAZ):
	global trips, links, legs, nodes
	def processNodes (**kwargs):
		'''kwargs: polys, nodes, st, end'''
		
		# Associate nodes to regions

		nodes = kwargs.get("nodes")
		polys = kwargs.get("polys")
		st = kwargs.get("st")
		end = kwargs.get("end")

		nMap = [0 for i in range(100000)]
		for i in range(st, end):
			# nodeMap[int(n["properties"]["id"])] = n["geometry"]["coordinates"]
			pos = geo.Point([nodes["x"][i], nodes["y"][i]])
			for i, poly in enumerate(polys):
				if pos.within(poly):
					nMap[int(nodes["NodeId"][i])] = i
					break
		return nMap
	def processLegs (**kwargs):
		'''kwargs: legs, nodeMap, linkMap, stL, endL'''

		# Associate 

		stL = kwargs.get("stL")
		endL = kwargs.get("endL")
		legs = kwargs.get("legs")
		nodeMap = kwargs.get("nodeMap")
		linkMap = kwargs.get("linkMap")
		legMap = {}
		ignored = 0
		for i in range(stL, endL):
			threadLock.acquire() # It seems like pandas dataframe (maybe numpy array?) don't support multi-threaded processes
			try: 
				pid = legs["PID"][i]
				leg_links = legs["LinkId"][i]
				legID = legs["Leg_ID"][i]
			except:
				ignored += 1
				continue
			threadLock.release()
			if not (pid in list(legMap.keys())):
				legMap[pid] = []
			st = 0
			end = 0
			try:
				if linkMap[int(float(leg_links[0]))][0] in linkMap[int(float(leg_links[1]))]:
					st = linkMap[int(float(leg_links[0]))][1]
				else:
					st = linkMap[int(float(leg_links[0]))][0]
				if linkMap[int(float(leg_links[-1]))][0] in linkMap[int(float(leg_links[-2]))]:
					end = linkMap[int(float(leg_links[-1]))][1]
				else:
					end = linkMap[int(float(leg_links[-1]))][0]
			except:
				ignored+=1
			stInd = nodeMap[st]
			endInd = nodeMap[end]
			if int(legID) == 1:
				legMap[pid].append([stInd, 0])
			else:
				legMap[pid].append([stInd, endInd])
		return [legMap, ignored]
	def processTrips (**kwargs):
		'''kwargs: trips, legMap, st, end'''
		trips = kwargs.get("trips")
		legMap = kwargs.get("legMap")
		st = kwargs.get("st")
		end = kwargs.get("end")
		for i in range(st, end):
			threadLock.acquire()
			pid = trips["PID"][i]
			tid = trips["Trip_ID"][i]
			stTime = trips["Start_time"][i]
			endTime = trips["End_time"][i]
			threadLock.release()
			stInd = int(legMap[str(pid)][int(tid)-1][0]) #facMap[actMap[trips["PID"][i]][trips["OriginAct"][i]]]
			endInd = int(legMap[str(pid)][int(tid)-1][1]) #facMap[actMap[trips["PID"][i]][trips["DestinationAct"][i]]]
			time = min((stTime + endTime) // 2, 24 * 60 * 60)
			timeFactor = int(max((time-1) // TIME_SEP, 0))
			startZoneInd = 0
			endZoneInd = 0
			if isTAZ:
				startZoneInd = stInd
				endZoneInd = endInd
			else:
				startZoneInd = timeFactor * len(polys) + stInd
				endZoneInd = timeFactor * len(polys) + endInd
			threadLock.acquire()
			# try:
			zones["features"][startZoneInd]["properties"]["Paths starting here"] += 1
			zones["features"][endZoneInd]["properties"]["Paths ending here"] += 1
			# except:
			# 	pass
			threadLock.release()
		return None
			
	threadLock = threading.Lock()

	print ("Creating visual: tripDensityByZone. IsTAZ = " + str(isTAZ))
	zones = {}
	print ("Copying zone information..")
	if isTAZ:
		zones = copy.deepcopy(TAZZones)
	else:
		zones = copy.deepcopy(neighborZones)
	polys = []
	for i, poly in enumerate(zones["features"]):
		# print(poly["geometry"]["coordinates"][0])
		polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
		poly["properties"]["Paths starting here"] = 0
		poly["properties"]["Paths ending here"] = 0
		poly["properties"]["ind"] = i
		if not isTAZ:
			poly["properties"]["time"] = "0:00:00"

	index = len(zones["features"])
	if not isTAZ:
		for t in range(math.ceil(24 * 60 * 60 / TIME_SEP)):
			if t == 0:
				continue
			for j in range(len(polys)):
				poly = zones["features"][j]
				zones["features"].append(copy.deepcopy(poly))
				zones["features"][index]["properties"]["ind"] = index
				zones["features"][index]["properties"]["time"] = "%d:%02d:%02d" %((t * TIME_SEP) // 3600, ((t*TIME_SEP) // 60) % 60, (t * TIME_SEP) % 60)
				index += 1
				if index % 1000 == 0:
					print ("Creating zones: " + str(index - len(polys)) + " / " + str(math.ceil(24 * 60 * 60 / TIME_SEP) * len(polys)))
	
	linkMap = [[] for i in range(100000)]
	for i in range(len(links["LinkId"])):
		linkMap[int(links["LinkId"][i])] = [int(links["fromLocationID"][i]), int(links["toLocationID"][i])]
	
	nodeMap = [0 for i in range(100000)]
	threads = []
	for i in range(NUM_THREADS):
		st = (i * len(nodes["NodeId"])) // NUM_THREADS
		end = ((i+1) * len(nodes["NodeId"])) // NUM_THREADS
		threads.append(processingThread(i, "node_thread_" + str(i), processNodes, polys=polys, nodes=nodes, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		for i, num in enumerate(thread.out):
			if num > 0:
				nodeMap[i] = num

	legMap = {}
	ignored = 0
	threads = []
	for i in range(NUM_THREADS):
		st = (i * len(legs["PID"])) // NUM_THREADS
		end = ((i+1) * len(legs["PID"])) // NUM_THREADS
		threads.append(processingThread(i, "leg_thread_" + str(i), processLegs, legs=legs, nodeMap=nodeMap, linkMap=linkMap, stL=st, endL=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		for legKey in thread.out[0].keys():
			if legKey in legMap:
				legMap[legKey] = legMap[legKey] + thread.out[0][legKey]
			else:
				legMap[legKey] = thread.out[0][legKey]
		ignored += thread.out[1]

	threads = []
	for i in range(NUM_THREADS):
		st = (i * len(trips["PID"])) // NUM_THREADS
		end = ((i+1) * len(trips["PID"])) // NUM_THREADS
		threads.append(processingThread(i, "trip_thread_" + str(i), processTrips, trips=trips, legMap=legMap, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
	with open (OUTPUT_FOLDER+"/tripDensityByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "w") as out:
		json.dump(zones, out, allow_nan=True)
def heatMap():
	global paths, links
	def processLinks (links):
		linkMap = {}
		for i in range(len(links["LinkId"])):
			linkId = str(int(links["LinkId"][i]))
			fromLoc = [links["fromLocationX"][i], links["fromLocationY"][i]]
			toLoc = [links["toLocationX"][i], links["toLocationY"][i]]
			linkMap[str(linkId)] = [fromLoc, toLoc]
		return linkMap
	def processPaths (**kwargs):
		'''kwargs: paths, linkMap, st, end'''
		paths = kwargs["paths"]
		linkMap = kwargs["linkMap"]
		st = kwargs["st"]
		end = kwargs["end"]
		out = []
		for i in range(st, end):
			threadLock.acquire()
			path = paths["path"][i]
			if i < 10:
				print(path)
			stTime = paths["departureTime"][i]
			endTime = paths["arrivalTime"][i]
			threadLock.release()
			nodeList = []
			n = 0
			try:
				stLink = linkMap[str(int(path[0]))]
				nextLink = linkMap[str(int(path[1]))]
			except:
				continue
			if stLink[0] in nextLink:
				n = 1
			formattedTime = "%02d:%02d:%02d" % (stTime // 3600, (stTime % 3600) // 60, stTime % 60)
			node = stLink[n]
			out.append (str(node[0]) + "," + str(node[1]) + "," + formattedTime)

			formattedTime = "%02d:%02d:%02d" % (endTime // 3600, (endTime % 3600) // 60, endTime % 60)
			node = linkMap[str(int(path[-1]))][n]
			out.append (str(node[0]) + "," + str(node[1]) + "," + formattedTime)
		print (out[:10])
		return out

	threadLock = threading.Lock()

	print ("Creating visual: heatMap")
	linkMap = processLinks(links)
	out = []
	out.append("Latitude,Longitude,Time")
	threads = []
	for i in range(NUM_THREADS):
		size = len(paths["path"])
		st = (i * size) // NUM_THREADS
		end = ((i+1) * size) // NUM_THREADS
		threads.append(processingThread(i, "path_thread_" + str(i), processPaths, paths=paths, linkMap=linkMap, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		out += thread.out
		
	with open (OUTPUT_FOLDER+"/heatMap.csv", "w") as outFile:
		for line in out:
			outFile.write(line + "\n")
def speedByLink(): #TODO: make updated version
	global links, speeds
def tripsByTime():
	global trips, links, activities
	def processLinks(links):
		linkMap = {}
		for i in range(len(links)):
			threadLock.acquire()
			linkId = str(int(links["LinkId"][i]))
			fromLoc = [links["fromLocationX"][i], links["fromLocationY"][i]]
			toLoc = [links["toLocationX"][i], links["toLocationY"][i]]
			threadLock.release()
			linkMap[str(linkId)] = [fromLoc, toLoc]
		return linkMap
	def processActivities(**kwargs):
		'''kwargs: activities, st, end'''
		activities = kwargs["activities"]
		st = kwargs["st"]
		end = kwargs["end"]
		actMap = {}
		for i in range(st, end):
			threadLock.acquire()
			pid = str(activities["PID"][i])
			actNum = str(activities["ActNum"][i])
			try:
				linkId = str(int(activities["LinkId"][i]))
			except:
				linkId = str(activities["LinkId"][i]) #Case: NaN
			threadLock.release()
			if not (actNum in actMap[pid]):
				actMap[pid] = {}
			actMap[pid][actNum] = linkId
			# print("test")
		# print(actMap)
		return actMap
	def processTrips (**kwargs):
		'''kwargs: linkMap, actMap, trips, st, end'''
		linkMap = kwargs["linkMap"]
		actMap = kwargs["actMap"]
		trips = kwargs["trips"]
		st = kwargs["st"]
		end = kwargs["end"]
		out = []
		print(actMap)
		for i in range(st, end):
			threadLock.acquire()
			pid = str(trips["PID"][i])
			orgAct = trips["OriginAct"][i]
			destAct = trips["DestinationAct"][i]
			stTime = trips["Start_time"][i]
			endTime = trips["End_time"][i]
			threadLock.release()
			time = (stTime + endTime) / 2
			# print(pid)
			
			startActPID = actMap[pid]
			startAct = startActPID[orgAct]
			startLink = linkMap[startAct]
			endLink = linkMap[actMap[pid][destAct]]
			startLoc = [(startLink[0][0] + startLink[1][0]) / 2, (startLink[0][1] + startLink[1][1]) / 2]
			endLoc = [(endLink[0][0] + endLink[1][0]) / 2, (endLink[0][1] + endLink[1][1]) / 2]
			out.append(startLoc[:] + endLoc[:] + [time])
		return out
	
	threadLock = threading.Lock()

	print("Creating visual: tripsArcs")
	out = []
	out.append("StartLat,StartLong,EndLat,EndLong,Time")
	linkMap = processLinks(links)
	actMap = {}
	threads = []
	for i in range(NUM_THREADS):
		size = len(activities["PID"])
		# print(activities)
		st = (i * size) // NUM_THREADS
		end = ((i+1) * size) // NUM_THREADS
		threads.append(processingThread(i, "activity_thread_" + str(i), processActivities, activities=activities, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		actMap.update(thread.out)

	out = []
	threads = []
	for i in range(NUM_THREADS):
		size = len(trips["PID"])
		st = (i * size) // NUM_THREADS
		end = ((i+1) * size) // NUM_THREADS
		threads.append(processingThread(i, "trip_thread_" + str(i), processTrips, linkMap=linkMap, actMap=actMap, trips=trips, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		out = out + thread.out
	with open (OUTPUT_FOLDER+"/tripsByTime.csv", "w") as outFile:
		for line in out:
			outFile.write(",".join(line) + "\n")
def followPeople():
	global activities, trips, links
	def processActivities(**kwargs):
		'''kwargs: activities, st, end'''
		activities = kwargs["activities"]
		st = kwargs["st"]
		end = kwargs["end"]
		actMap = {}
		for i in range(st, end):
			threadLock.acquire()
			pid = activities["PID"][i]
			actNum = activities["ActNum"][i]
			try:
				linkId = str(int(activities["LinkId"][i]))
			except:
				linkId = str(activities["LinkId"][i]) #Case: NaN
			threadLock.release()
			if not (actNum in actMap[pid]):
				actMap[pid] = {}
			actMap[pid][actNum] = linkId

			# print("test")
		print(actMap)
		return actMap #ouput: {PID: {actID: linkID}}
	def processLinks(links):
		linkMap = {}
		for i in range(len(links)):
			threadLock.acquire()
			linkId = str(int(links["LinkId"][i]))
			fromLoc = [links["fromLocationX"][i], links["fromLocationY"][i]]
			toLoc = [links["toLocationX"][i], links["toLocationY"][i]]
			threadLock.release()
			linkMap[str(linkId)] = [fromLoc, toLoc]
		print(linkMap)

		return linkMap

	'''
	Plan:
	out[PID] = trip every 10 mins. Orig + dest might be the same if person stays in place.
	'''
	
	print("Creating visual: followPeople")
	out = {}
	threadLock = threading.Lock()
	tempMap = {}
	for i in range(len(trips["PID"])):
		pid = trips["PID"][i]
		if pid in tempMap:
			tempMap[pid] += 1
		else:
			tempMap[pid] = 1
	followedPIDs = []
	for key in tempMap.keys():
		if len(followedPIDs) < 100:
			followedPIDs.append(key)
			out[key] = [[] for i in range(24*6)]
	linkMap = processLinks(links)
	actMap = {}
	threads = []
	for i in range(NUM_THREADS):
		size = len(activities["PID"])
		st = (i * size) // NUM_THREADS
		end = ((i+1) * size) // NUM_THREADS
		threads.append(processingThread(i, "activity_thread_" + str(i), processActivities, activities=activities, st=st, end=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
		actMap.update(thread.out)

	for i in range(len(activities["PID"])):
		pid = activities['PID'][i]
		if not (pid in followedPIDs):
			continue
		stTime = activities["Start_time"][i]
		endTime = activities["End_time"][i]
		orgAct = activities["OriginAct"][i]
		destAct = activities["DestinationAct"][i]
		avgTime = (stTime + endTime) / 2
		time = "%d:%02d:%02d" %(avgTime // 3600, (avgTime // 60) % 60, avgTime % 60)
		out[max(avgTime // (24*6), 24*6-1)] = [str(pid), ",".join(linkMap[actMap[pid][orgAct]][0]), ",".join(linkMap[actMap[pid][destinationAct][1]]), str(time)]
	with open (OUTPUT_FOLDER+"/followPerson.csv", "w") as outFile:
		for person in out:
			for time in person:
				outFile.write(",".join(time) + "\n") 


# speedPerLink(LINK_FILE, DATA_FILE)
# print ("Data parsed")


travelTimesByZone()
costsByZone(True)
costsByZone(False)
modeShareByZone(True)
modeShareByZone(False)
speedByZone(True)
speedByZone(False)
VMTByZone(True)
VMTByZone(False)
occupancyByZone(True)
occupancyByZone(False)
timeDelayByZone(True)
timeDelayByZone(False)
tripDensityByZone(True)
tripDensityByZone(False)
heatMap()
# tripsByTime()
# followPeople()
# print(activities)
# scenario = "sf_light-100k"
# with open("simul_ids.txt", "w") as file:
# 	file.write(str(database.get_simul_by_scenario(scenario)))
# with open("scenarios.txt", "w") as file:
# 	df = database.load_simulation_df()
# 	file.write(str(df["simulation_id"])  + str(df["scenario"]))
