# TODO: verify that mode / neighborhood works √
#		rename cols to understandable names
#		Create a to/from map to see if it's consistent
#		Fix visualizations ~ 


NODE_FILE = "../input_files/sf_light_nodes_geometry.geojson"
LINK_FILE = "../input_files/sf_light_link_geometry.geojson"
DATA_FILE = "../input_files/sf_light_50k_BAU_link_stats_by_hour.csv"
OUTPUT_LINK_FILE = "../output_files/delay_neighbors.json"
ZONE_FILE = "../input_files/shapefiles/SF_Neighborhoods/shape.json" #SF_Neighborhoods // TAZ_SF
MODES = "walk, bus, bike, walk_transit, drive_transit, ridehail_transit, ride_hail, ridehail_pooled, car".split(", ")
MODE_GROUPS = "active, car, ridehail, transit".split(", ") #pedestrian, car, ridehail, public transport
MODE_GROUP_DICT = {"walk": 0, "bike": 0, "walk_transit": 3, "drive_transit": 3, "ridehail_transit": 3, "ride_hail": 2, "ridehail_pooled":2, "car":1, "bus": 3}
TIME_SEP = 14400 # in seconds
INCOME_SEP = 50000 # 0 - 200 000
NUM_THREADS = 4

import os, sys, threading, copy, json, math
sys.path.append(os.path.abspath("/Users/git/BISTRO_Dashboard/BISTRO_Dashboard"))
os.chdir('/Users/Timothe/Downloads/SF_light/scripts') # Cause VScode is weird

from db_loader import BistroDB
import numpy as np
import pandas as pd
import shapely.geometry as geo

def loadDB (simulation_id):
	database = BistroDB('bistro', 'bistroclt', 'client', '13.56.123.155')
	# submission = submission_id
	# simulation = database.load_simulation_df()[database.load_simulation_df()['scenario'] == 'sf_light-50k']
	# simulation_id = simulation.loc[0, 'simulation_id']
	simulation_id = 'db21069e-d19b-11ea-bfff-faffc250aee5'# simulation_id for BAU 06/25 BEAM output
	scenario = "sf_light-50k" #simulation.iloc[0, 2]
	simulation_id = [simulation_id]
	return (database, scenario, simulation_id)
def loadLegs(db, simulation_id):
	print("Loading legs...", end=" ")

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
	print("Loading activities...", end=" ")
	acts = db.load_activities(scenario)
	print("activities loaded")
	return acts
def loadFacilities(db, scenario):
	print("Loading facilities...", end=" ")
	facs = db.load_facilities(scenario)
	print("facilities loaded")
	return facs
def loadLinks(db, scenario):
	print("Loading links...", end=" ")
	links = db.load_links(scenario)
	print("links loaded")
	return links
def loadPaths(db, simulation_id, scenario):
	print("Loading paths...", end=" ")
	paths = db.load_paths(simulation_id, scenario)
	print("paths loaded")
	return paths
def loadTrips (db, simulation_id):
	print("Loading trips...", end=" ")

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
	print("Loading population...", end=" ")
	people = db.load_person(scenario)
	print("population loaded")
	return people
def loadVehicles (db, scenario):
	print("Loading vehicles...", end=" ")
	vehicles = db.load_vehicles(scenario)
	print("vehicles loaded")
	return vehicles
def loadVehicleTypes (db, scenario):
	print("Loading vehicles types...", end=" ")
	vehicles = db.load_vehicle_types(scenario)
	print("vehicles types loaded")
	return vehicles
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

def travelTimesByZone(submission, node_file, zone_file):
	(database, scenario, simulation_id) = loadDB(submission)
	legs = loadLegs(database, simulation_id)
	links = loadLinks(database, scenario)

	with open(zone_file, "r") as zoneFile:
		zones = json.load(zoneFile)
	with open(node_file, "r") as nodeFile:
		nodes = json.load(nodeFile)
	print("Files opened")

	polys = []
	for i, poly in enumerate(zones["features"]):
		# print(poly["geometry"]["coordinates"][0])
		polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
		poly["properties"]["from"] = "all"
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
						zones["features"][index]["properties"]["from"] = "all"
					else:
						zones["features"][index]["properties"]["from"] = zones["features"][i-1]["properties"]["name"]
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
	for n in nodes["features"]:
		nodeMap[int(n["properties"]["id"])] = n["geometry"]["coordinates"]


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
def costsByZone (submission, node_file, zone_file, isTAZ):
	db, scenario, simulation_id = loadDB(submission)
	trips = loadTrips(db, simulation_id)
	links = loadLinks(db, scenario)
	population = loadPopulation(db, scenario)
	legs = loadLegs(db, simulation_id)
	# acts = loadAct(db, scenario)
	# facilities = loadFacilities(db, scenario)
	# print(factilities.columns)
	# return 
	with open(zone_file, "r") as zoneFile:
		zones = json.load(zoneFile)
	with open(node_file, "r") as nodeFile:
		nodes = json.load(nodeFile)
	print("Files opened")

	polys = []
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
			poly["properties"]["from"] = "all"

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
						zones["features"][index]["properties"]["from"] = "all"
					else:
						zones["features"][index]["properties"]["from"] = zones["features"][i-1]["properties"]["name"]
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
	for n in nodes["features"]:
		nodeMap[int(n["properties"]["id"])] = n["geometry"]["coordinates"]
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
	
	return zones
def modeShareByZone(submission, node_file, zone_file, isTAZ):

	#Run as multithread
	def processNodes (**kwargs):
		'''kwargs: polys, nodes, st, end'''
		nodes = kwargs.get("nodes")
		polys = kwargs.get("polys")
		st = kwargs.get("st")
		end = kwargs.get("end")

		nMap = [0 for i in range(100000)]
		for node in nodes["features"][st:end]:
			# nodeMap[int(n["properties"]["id"])] = n["geometry"]["coordinates"]
			pos = geo.Point(node["geometry"]["coordinates"])
			for i, poly in enumerate(polys):
				if pos.within(poly):
					nMap[int(node["properties"]["id"])] = i
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

	db, scenario, simulation_id = loadDB(submission)
	trips = loadTrips(db, simulation_id)
	links = loadLinks(db, scenario)
	population = loadPopulation(db, scenario)
	legs = loadLegs(db, simulation_id)

	if os.path.exists("../output_files/missingPIDs.csv"):
		os.remove("../output_files/missingPIDs.csv")
	with open(zone_file, "r") as zoneFile:
		zones = json.load(zoneFile)
	with open(node_file, "r") as nodeFile:
		nodes = json.load(nodeFile)
	print("Files opened")

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
			poly["properties"]["from"] = "all"

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
						zones["features"][index]["properties"]["from"] = "all"
					else:
						zones["features"][index]["properties"]["from"] = zones["features"][i-1]["properties"]["name"]
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
		st = (i * len(nodes["features"])) // NUM_THREADS
		end = ((i+1) * len(nodes["features"])) // NUM_THREADS
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
			
	
	return zones
def speedByLink(link_file, speed_file):
	with open(link_file, "r") as linkFile:
		links = json.load(linkFile)
	with open(speed_file, "r") as speedFile:
		speeds = speedFile.readlines()
	print("Files opened")
	
	out = []
	index = 0
	for t in range(math.ceil(24 * 60 * 60 / TIME_SEP)):
		for i in range(len(links["features"])):
			out.append({})
			out[index]["LinkID"] = links["features"][i]["properties"]["link"]
			out[index]["coords"] = links["features"][i]["geometry"]["coordinates"][:]
			out[index]["totSpeed"] = 0
			out[index]["numVals"] = 0
			out[index]["avgSpeed"] = None
			out[index]["time"] = "%d:%02d:%02d" %((t * TIME_SEP) // 3600, ((t*TIME_SEP) // 60) % 60, (t * TIME_SEP) % 60)
			index += 1
	linkMap = list(map(lambda x: str(x["properties"]["link"]), links["features"]))
	for l in speeds[1:]:
		line = l.split(",")
		speed = float(line[11])
		linkID = str(line[1])
		hour = float(line[4])
		timeFactor = int(max((hour * 3600 - 1) // TIME_SEP, 0))
		linkInd = linkMap.index(linkID)
		out[timeFactor * len(links["features"]) + linkInd]["totSpeed"] += speed
		out[timeFactor * len(links["features"]) + linkInd]["numVals"] += 1
	
	for link in out:
		if link["numVals"] > 0:
			link["avgSpeed"] = link["totSpeed"] / link["numVals"]
	
	with open(OUTPUT_LINK_FILE, "w") as outFile:
		outFile.write("ID,totSpeed,numVals,avgSpeed,time,fromLat,fromLong,toLat,toLong\n")
		for link in out:
			outFile.write(",".join(list(map(lambda x: str(x), [link["LinkID"], link["totSpeed"], link["numVals"], link["avgSpeed"], link["time"], link["coords"][0][0], link["coords"][0][1], link["coords"][1][0], link["coords"][1][1]]))) + "\n")
def speedByZone(zone_file, link_file, speed_file, node_file, isTAZ):
	def processNodes (**kwargs):
		'''kwargs: polys, nodes, st, end'''
		nodes = kwargs.get("nodes")
		polys = kwargs.get("polys")
		st = kwargs.get("st")
		end = kwargs.get("end")

		nMap = [0 for i in range(100000)]
		for node in nodes["features"][st:end]:
			# nodeMap[int(n["properties"]["id"])] = n["geometry"]["coordinates"]
			pos = geo.Point(node["geometry"]["coordinates"])
			for i, poly in enumerate(polys):
				if pos.within(poly):
					nMap[int(node["properties"]["id"])] = i
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
	
	with open(zone_file, "r") as zoneFile:
		zones = json.load(zoneFile)
	with open(speed_file, "r") as speedFile:
		speeds = speedFile.readlines()
	with open(node_file, "r") as nodeFile:
		nodes = json.load(nodeFile)
	print("Files opened")

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
		st = (i * len(nodes["features"])) // NUM_THREADS
		end = ((i+1) * len(nodes["features"])) // NUM_THREADS
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
	return zones
def VMTByZone (zone_file, link_file, data_file, node_file, isTAZ):
	def processNodes (**kwargs):
		'''kwargs: polys, nodes, st, end'''
		nodes = kwargs.get("nodes")
		polys = kwargs.get("polys")
		st = kwargs.get("st")
		end = kwargs.get("end")

		nMap = [0 for i in range(100000)]
		for node in nodes["features"][st:end]:
			# nodeMap[int(n["properties"]["id"])] = n["geometry"]["coordinates"]
			pos = geo.Point(node["geometry"]["coordinates"])
			for i, poly in enumerate(polys):
				if pos.within(poly):
					nMap[int(node["properties"]["id"])] = i
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

	with open(zone_file, "r") as zoneFile:
		zones = json.load(zoneFile)
	with open(data_file, "r") as dataFile:
		data = dataFile.readlines()
	with open(node_file, "r") as nodeFile:
		nodes = json.load(nodeFile)
	print("Files opened")

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
		st = (i * len(nodes["features"])) // NUM_THREADS
		end = ((i+1) * len(nodes["features"])) // NUM_THREADS
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
		st = max((i * len(data)) // NUM_THREADS, 1)
		end = ((i+1) * len(data)) // NUM_THREADS
		threads.append(processingThread(i, "data_thread_" + str(i), processVMT, data=data, polys=polys, stP=st, endP=end))
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()

	return zones
def occupancyByZone(submission, node_file, zone_file, isTAZ):
	def processNodes (**kwargs):
		'''kwargs: polys, nodes, st, end'''
		nodes = kwargs.get("nodes")
		polys = kwargs.get("polys")
		st = kwargs.get("st")
		end = kwargs.get("end")

		nMap = [0 for i in range(100000)]
		for node in nodes["features"][st:end]:
			# nodeMap[int(n["properties"]["id"])] = n["geometry"]["coordinates"]
			pos = geo.Point(node["geometry"]["coordinates"])
			for i, poly in enumerate(polys):
				if pos.within(poly):
					nMap[int(node["properties"]["id"])] = i
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

	db, scenario, simulation_id = loadDB(submission)
	paths = loadPaths(db, simulation_id, scenario)
	vehicles = loadVehicles(db, scenario)
	vehicleTypes = loadVehicleTypes(db, scenario)
	links = loadLinks(db, scenario)
	
	with open(zone_file, "r") as zoneFile:
		zones = json.load(zoneFile)
	with open(node_file, "r") as nodeFile:
		nodes = json.load(nodeFile)
	print("Files opened")

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
		st = (i * len(nodes["features"])) // NUM_THREADS
		end = ((i+1) * len(nodes["features"])) // NUM_THREADS
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
	
	return zones
def timeDelayByZone(submission, node_file, zone_file, isTAZ):
	def processNodes (**kwargs):
		'''kwargs: polys, nodes, st, end'''
		nodes = kwargs.get("nodes")
		polys = kwargs.get("polys")
		st = kwargs.get("st")
		end = kwargs.get("end")

		nMap = [0 for i in range(100000)]
		for node in nodes["features"][st:end]:
			# nodeMap[int(n["properties"]["id"])] = n["geometry"]["coordinates"]
			pos = geo.Point(node["geometry"]["coordinates"])
			for i, poly in enumerate(polys):
				if pos.within(poly):
					nMap[int(node["properties"]["id"])] = i
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

	db, scenario, simulation_id = loadDB(submission)
	paths = loadPaths(db, simulation_id, scenario)
	population = loadPopulation(db, scenario)
	links = loadLinks(db, scenario)

	with open(zone_file, "r") as zoneFile:
		zones = json.load(zoneFile)
	with open(node_file, "r") as nodeFile:
		nodes = json.load(nodeFile)
	print("Files opened")

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
			poly["properties"]["from"] = "all"

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
						zones["features"][index]["properties"]["from"] = "all"
					else:
						zones["features"][index]["properties"]["from"] = zones["features"][i-1]["properties"]["name"]
					zones["features"][index]["properties"]["ind"] = index
					index += 1
					if index % 1000 == 0:
						print ("Creating zones: " + str(index) + " / " + str(len(polys) * len(MODES) * len(polys)+1))

	nodeMap = [0 for i in range(100000)]
	threads = []
	for i in range(NUM_THREADS):
		st = (i * len(nodes["features"])) // NUM_THREADS
		end = ((i+1) * len(nodes["features"])) // NUM_THREADS
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
	
	return zones
	

# speedPerLink(LINK_FILE, DATA_FILE)
# print ("Data parsed")


#Write to json file
zones = timeDelayByZone("db21069e-d19b-11ea-bfff-faffc250aee5", NODE_FILE, ZONE_FILE, False)

with open(OUTPUT_LINK_FILE, "w") as out:
	json.dump(zones, out)
print ("Output written to " + OUTPUT_LINK_FILE)