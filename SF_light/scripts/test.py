'''
TODO:
- Add units in label names
- Wrap-up + summary
'''
import sys, os, threading, copy, json, math

from db_loader import BistroDB
import numpy as np
import runVis as rv
import pandas as pd
import shapely.geometry as geo
import processingThread as pt

class Visualization:
	# These are default values. Modify options.txt instead if possible
	DATA_FILE = ""
	NEIGHBOR_ZONE_FILE = ""
	TAZ_ZONE_FILE = ""
	MODES = []
	MODE_GROUPS = []
	MODE_GROUP_DICT = {}
	TIME_SEP = 0
	INCOME_SEP = 0
	NUM_THREADS = 0
	SIMUL_ID = ""
	OUTPUT_FOLDER = ""
	# 32cfbb84-39ce-11eb-9ec7-9801a798306b : cordon sioux-faux
	# 01729e42-41cd-11eb-94f5-9801a798306b : per-mile sioux-faux
	# 613dfaba-6a36-11eb-a978-06b502d4a7f7 : free-form cordon sioux-faux


	def __init__(self, simulation_id):
		
		Visualization.SIMUL_ID = simulation_id
		Visualization.OUTPUT_FOLDER = "../output_files/"
		
		try: # create dir if it doesn't already exist
			os.mkdir(Visualization.OUTPUT_FOLDER)
		except:
			pass
		Visualization.OUTPUT_FOLDER +="{}".format(simulation_id)
		try:
			os.mkdir(Visualization.OUTPUT_FOLDER)
		except:
			pass
		# Read options file
		with open("options.txt", "r") as optionsFile:
			for line in optionsFile.readlines():
				words = line[:-1].split(" ")
				if len(words) <= 1:
					continue
				selector = words[0]
				if selector[0] == "#":
					continue
				elif selector == "DATA_PATH:":
					Visualization.DATA_FILE = words[1]
				elif selector == "NEIGHBOR_ZONE_PATH:":
					Visualization.NEIGHBOR_ZONE_FILE = words[1]
				elif selector == "TAZ_ZONE_PATH:":
					Visualization.TAZ_ZONE_FILE = words[1]
				elif selector == "MODES:":
					value = "".join(words[1:]) # Some wizardry to turn comma seperated values into array
					Visualization.MODES = value.split(",")
				elif selector == "MODE_GROUPS:":
					value = "".join(words[1:])
					Visualization.MODE_GROUPS = value.split(",")
				elif selector == "MODE_GROUP_DICT:":
					value = "".join(words[1:])
					Visualization.MODE_GROUP_DICT = eval(value)
				elif selector == "TIME_SEP:":
					Visualization.TIME_SEP = int(words[1])
				elif selector == "INCOME_SEP:":
					Visualization.INCOME_SEP = int(words[1])
				elif selector == "NUM_THREADS:":
					Visualization.NUM_THREADS = int(words[1])
				else:
					raise ValueError ("Unknown value in options.txt at {}".format(line))

	# Are these pointless? yes
	# Am I still going to keep them? yes
	def loadDB (self, simulation_id):
		database = BistroDB('bistro', 'bistroclt', 'client', '52.53.200.197')
		simulation_id = [simulation_id]
		scenario = database.get_scenario(simulation_id)
		return (database, scenario, simulation_id)
	def loadLegs(self, db, simulation_id):
		print("Loading legs...")
		legs = db.load_legs(simulation_id, links=True)
		print("legs loaded")
		return legs
	def loadAct(self, db, scenario):
		print("Loading activities...")
		acts = db.load_activities(scenario)
		print("activities loaded")
		return acts
	def loadLinks(self, db, scenario):
		print("Loading links...")
		links = db.load_links(scenario)
		print("links loaded")
		return links
	def loadPaths(self, db, simulation_id, scenario):
		print("Loading paths...")
		paths = db.load_paths(simulation_id, scenario)
		print("paths loaded")
		return paths
	def loadTrips (self, db, simulation_id):
		print("Loading trips...")	
		trips = db.load_trips(simulation_id)
		print("trips loaded")
		return trips
	def loadPopulation(self, db, scenario):
		print("Loading population...")
		people = db.load_person(scenario)
		print("population loaded")
		return people
	def loadVehicles (self, db, scenario):
		print("Loading vehicles...")
		vehicles = db.load_vehicles(scenario)
		print("vehicles loaded")
		return vehicles
	def loadVehicleTypes (self, db, scenario):
		print("Loading vehicles types...")
		vehicles = db.load_vehicle_types(scenario)
		print("vehicles types loaded")
		return vehicles
	def loadNodes (self, db, scenario):
		print("Loading nodes...")
		nodes = db.load_nodes(scenario)
		print("nodes loaded")
		return nodes

	def loadTables(self):
		# Load all the tables
		# All of them (at least the ones we need)
		(database, scenario, simulation_id) = self.loadDB (self.SIMUL_ID)
		try:
			self.legs = self.loadLegs(database, simulation_id)
			self.links = self.loadLinks(database, scenario)
			self.trips = self.loadTrips(database, simulation_id)
			self.person = database.load_person(scenario)
			self.population = self.loadPopulation(database, scenario)
			self.vehicles = self.loadVehicles(database, scenario)
			self.activities = self.loadAct(database, scenario)
			self.vehicleTypes = self.loadVehicleTypes(database, scenario)
			self.paths = self.loadPaths(database, simulation_id, scenario)
			self.nodes = self.loadNodes(database, scenario)
		except Exception as e:
			# the simul_id is missing some tables
			print ("=====THE DATABASE IS MISSING REQUIRED TABLES FOR THIS SIMULATION ID. SOME VISUALIZATIONS WILL BREAK=====")
			print ("Error information:\n" + str(e))
			inp = str(input("CONTINUE? (y/n): "))
			if (inp != "y"):
				sys.exit()
		print("Table queries finished")

		# IMPORTANT: These need to be in GeoJSON format. I created test_json.py to handle this.
		with open(Visualization.NEIGHBOR_ZONE_FILE, "r") as neighborZoneFile:
			self.neighborZones = json.load(neighborZoneFile)
		with open(Visualization.TAZ_ZONE_FILE, "r") as TAZZoneFile:
			self.TAZZones = json.load(TAZZoneFile)
		print ("Files loaded")

	def travelTimesByZone(self,isTAZ):
		# This visual allows to see travel times starting at certain zones
		print ("Creating visual: travelTimesByZone")
		zones = {}
		# print ("Copying zone information..")
		if isTAZ:
			zones = copy.deepcopy(self.TAZZones)
		else:
			zones = copy.deepcopy(self.neighborZones)
		polys = []
		# We moddel zones as polygons, which we'll then use to match points to zones

		for i, poly in enumerate(zones["features"]):
			polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
			# These identify the specific polygon
			poly["properties"]["origin"] = "all"
			poly["properties"]["ind"] = i
			poly["properties"]["mode"] = Visualization.MODE_GROUPS[0]
			poly["properties"]["name"] = poly["properties"]["TAZCE10"]
			poly["properties"]["time"] = "0:00:00"

			# These are the values stored by the polygon
			poly["properties"]["ttotTime"] = 0
			poly["properties"]["tnumNodes"] = 0
			poly["properties"]["average time to (s)"] = None
			poly["properties"]["ftotTime"] = 0
			poly["properties"]["fnumNodes"] = 0
			poly["properties"]["average time from (s)"] = None
			poly["properties"]["average time (s)"] = None

			

		#Duplicate map for every node
		index = len(zones["features"])
		# Time filter
		for t in range(math.ceil(24 * 60 * 60 / Visualization.TIME_SEP)+1):
			# Origin filter
			for o in range(len(Visualization.MODE_GROUPS)):
				# Income filter
				for i in range(len(polys)+1):
					# Initial already covered above
					if t == 0 and o == 0 and i == 0:
						continue
					for j in range(len(polys)):
						poly = zones["features"][j]
						zones["features"].append(copy.deepcopy(poly))
						
						# Set indicators to identify polygon
						if i == 0:
							zones["features"][index]["properties"]["origin"] = "all"
						else:
							zones["features"][index]["properties"]["origin"] = zones["features"][i-1]["properties"]["name"]
						zones["features"][index]["properties"]["mode"] = Visualization.MODE_GROUPS[o]
						zones["features"][index]["properties"]["ind"] = index
						timeSeconds = (t+1) * Visualization.TIME_SEP
						zones["features"][index]["properties"]["time"] = "%d:%02d:%02d" %(timeSeconds // 3600, (timeSeconds // 60) % 60, timeSeconds % 60)
						index += 1
						# Debug
						# if index % 1000 == 0:
						# 	print ("Creating zones: " + str(index - len(polys)) + " / " + str(len(polys) * len(polys) * len(Visualization.MODES) * math.ceil(24 * 60 * 60 / Visualization.TIME_SEP)))
		
		# Associate [linkID:[nodeID, nodeID]]
		linkMap = [[] for i in range(100000)]
		for i in range(len(self.links["LinkId"])):
			linkMap[int(self.links["LinkId"][i])] = [int(self.links["fromLocationID"][i]), int(self.links["toLocationID"][i])]
		
		# Associate [nodeID:[lat,long]
		nodeMap = [[] for i in range(100000)]
		for i in range(len(self.nodes["NodeId"])):
			nodeMap[int(self.nodes["NodeId"][i])] = [self.nodes["x"][i], self.nodes["y"][i]]


		#Parse input per leg
		ignored = 0
		ignored_modes = []
		for i in range(len(self.legs["PID"])):
			# debug
			# if i % 1000 == 0:
			# 	print("Parsing legs: " + str(i) + " / " + str(len(self.legs["LinkId"])))
			leg_links = self.legs["LinkId"][i]
			
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
				modeFactor = Visualization.MODE_GROUP_DICT[self.legs["Mode"][i]]
			except:
				ignored+=1
				if not (self.legs["Mode"][i] in ignored_modes):
					ignored_modes.append(self.legs["Mode"][i])
				continue
			# We need to find the corresponding zone in a 1D array, so we'll need a few factors
			timeFactor = ((int(self.legs["End_time"][i]) + int(self.legs["Start_time"][i])) // (2 * Visualization.TIME_SEP))
			
			# Different indexes
			ind = timeFactor * len(Visualization.MODE_GROUPS) * (len(polys)+1) * len(polys) + modeFactor * (len(polys)+1) * len(polys) + (endInd+1) * len(polys) + stInd
			all_modeInd = timeFactor * len(Visualization.MODE_GROUPS) * (len(polys)+1) * len(polys) + (endInd+1) * len(polys) + stInd
			all_fromInd = timeFactor * len(Visualization.MODE_GROUPS) * (len(polys)+1) * len(polys) + modeFactor * (len(polys)+1) * len(polys) + stInd
			all_allInd = timeFactor * len(Visualization.MODE_GROUPS) * (len(polys)+1) * len(polys) + stInd 
			inds = [ind, all_modeInd, all_fromInd, all_allInd]

			time = int(self.legs["End_time"][i]) - int(self.legs["Start_time"][i])
			for o, index in enumerate(inds):
				zones["features"][index]["properties"]["ttotTime"] += time
				zones["features"][index]["properties"]["tnumNodes"] += 1
			
			# Same as before, but this time calculating time to get anywhere starting from that node
			ind = timeFactor * len(Visualization.MODE_GROUPS) * (len(polys)+1) * len(polys) + modeFactor * (len(polys)+1) * len(polys) + (stInd+1) * len(polys) + endInd
			all_modeInd = timeFactor * len(Visualization.MODE_GROUPS) * (len(polys)+1) * len(polys) + (stInd+1) * len(polys) + endInd
			all_fromInd = timeFactor * len(Visualization.MODE_GROUPS) * (len(polys)+1) * len(polys) + modeFactor * (len(polys)+1) * len(polys) + endInd
			all_allInd = timeFactor * len(Visualization.MODE_GROUPS) * (len(polys)+1) * len(polys) + endInd 
			inds = [ind, all_modeInd, all_fromInd, all_allInd]
			for o, index in enumerate(inds):
				zones["features"][index]["properties"]["ftotTime"] += time
				zones["features"][index]["properties"]["fnumNodes"] += 1
		
		# Debug
		# print ("Legs parsed: " + str(len(self.legs["PID"]) - ignored))
		# print("Legs ignored: " + str(ignored))
		# print ("Modes ignored: " + ", ".join(ignored_modes))
		if ignored * 2 >= len(self.legs["PID"]) and not ("y" in str(input("***MORE THAN 50% OF THE LEGS TABLE WAS NOT USABLE. THIS IS MOST LIKELY CAUSED BY A BROKEN TABLE. CONTINUE? y/n***   ")).lower()):
			return 0

		# Round to 0.1
		for i, poly in enumerate(zones["features"]):
			if poly["properties"]["tnumNodes"] > 0:
				poly["properties"]["average time to (s)"] = round(poly["properties"]["ttotTime"] / poly["properties"]["tnumNodes"] * 10) / 10
			if poly["properties"]["fnumNodes"] > 0:
				poly["properties"]["average time from (s)"] = round(poly["properties"]["ftotTime"] / poly["properties"]["fnumNodes"] * 10) / 10
			if poly["properties"]["tnumNodes"] + poly["properties"]["fnumNodes"] > 0:
				poly["properties"]["average time (s)"] = round((poly["properties"]["ttotTime"] + poly["properties"]["ftotTime"]) / (poly["properties"]["tnumNodes"] + poly["properties"]["fnumNodes"]) * 10) / 10
		with open (Visualization.OUTPUT_FOLDER+"/travelTimes.json", "w") as out:
			json.dump(zones, out, allow_nan=True)
		rv.createVisual(Visualization.OUTPUT_FOLDER+"/travelTimes.json", "../input_files/configs/travelTimes.json", "../visualizations/temp/travelTimes.html", "../input_files/circle_params.txt")
		
	def costsByZone (self, isTAZ, useStartPoints=True):
		print ("Creating visual: costsByZone. IsTAZ = " + str(isTAZ))
		polys = []
		zones = {}
		# print ("Copying zone information..")
		if isTAZ:
			zones = copy.deepcopy(self.TAZZones)
		else:
			zones = copy.deepcopy(neighborZones)
		for i, poly in enumerate(zones["features"]):
			polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
			poly["properties"]["mode"] = Visualization.MODES[0]
			poly["properties"]["total cost ($)"] = 0
			poly["properties"]["numVals"] = 0
			poly["properties"]["average cost ($)"] = None
			poly["properties"]["ind"] = i
			if isTAZ:
				poly["properties"]["income"] = "0-" + str(Visualization.INCOME_SEP)
				poly["properties"]["time"] = "0:00:00"
			else:
				poly["properties"]["origin"] = "all"

		index = len(zones["features"])
		# Create zones for the filters
		if isTAZ:
			for t in range(math.ceil(24 * 60 * 60 / Visualization.TIME_SEP)):
				for inc in range(math.ceil(200000 / Visualization.INCOME_SEP)):
					for o in range(len(Visualization.MODES)):
						if t == 0 and inc == 0 and o == 0:
							continue
						for j in range(len(polys)):
							poly = zones["features"][j]
							# Zone properties
							zones["features"].append(copy.deepcopy(poly))
							zones["features"][index]["properties"]["mode"] = Visualization.MODES[o]
							zones["features"][index]["properties"]["ind"] = index
							zones["features"][index]["properties"]["income"] = ( str(inc * Visualization.INCOME_SEP) if inc == 0 else str(inc * Visualization.INCOME_SEP + 1) ) + "-" + str((inc+1) * Visualization.INCOME_SEP)
							timeSeconds = t * Visualization.TIME_SEP
							formatTime = "%d:%02d:%02d" %(timeSeconds // 3600, (timeSeconds // 60) % 60, timeSeconds % 60)
							zones["features"][index]["properties"]["time"] = formatTime
							index += 1
							#debug
							# if index % 1000 == 0:
							# 	print ("Creating zones: " + str(index - len(polys)) + " / " + str(math.ceil(200000 / Visualization.INCOME_SEP) * math.ceil(24 * 60 * 60 / Visualization.TIME_SEP) * len(Visualization.MODES) * len(polys)))
		else:
			# If going by neighborhood, different filters are used
			for o in range(len(Visualization.MODES)):
				for i in range(len(polys)+1):
					if o == 0 and i == 0:
						continue
					for j in range(len(polys)):
						poly = zones["features"][j]
						zones["features"].append(copy.deepcopy(poly))
						if i == 0:
							zones["features"][index]["properties"]["origin"] = "all"
						else:
							zones["features"][index]["properties"]["origin"] = zones["features"][i-1]["properties"]["name"]
						zones["features"][index]["properties"]["mode"] = Visualization.MODES[o]
						zones["features"][index]["properties"]["ind"] = index
						# zones["features"][index]["properties"]["income"] = ( str(inc * Visualization.INCOME_SEP) if inc == 0 else str(inc * Visualization.INCOME_SEP + 1) ) + "-" + str((inc+1) * Visualization.INCOME_SEP)
						# zones["features"][index]["properties"]["time"] = 1
						index += 1
						# if index % 1000 == 0:
						# 	print ("Creating zones: " + str(index - len(polys)) + " / " + str(len(polys) * len(polys) * len(Visualization.MODES))) # * math.ceil(24 * 60 * 60 / Visualization.TIME_SEP)))
		
		# Associate [linkID:[fromID, toID]]
		linkMap = [[] for i in range(100000)]
		for i in range(len(self.links["LinkId"])):
			linkMap[int(self.links["LinkId"][i])] = [int(self.links["fromLocationID"][i]), int(self.links["toLocationID"][i])]
		
		# Associate [nodeID:[long, lat]]
		nodeMap = [[] for i in range(100000)]
		for i in range(len(self.nodes["NodeId"])):
			nodeMap[int(self.nodes["NodeId"][i])] = [self.nodes["x"][i], self.nodes["y"][i]]
		
		# Associate {PID: income}
		popMap = {}
		for i in range(len(self.population["PID"])):
			popMap[self.population["PID"][i]] = int(self.population["income"][i])

		legMap = {} # {PID: [tripNum:[st, end]]}
		ignored = 0
		for i in range(len(self.legs["PID"])): 
			# if i % 1000 == 0:
			# 	print ("Parsing legs: " + str(i) + " / " + str(len(self.legs["PID"])))
			pid = self.legs["PID"][i]
			if not (pid in list(legMap.keys())):
				legMap[pid] = []
			leg_links = self.legs["LinkId"][i]
			st = 0
			end = 0
			try:
				if linkMap[int(leg_links[0])][0] in linkMap[int(leg_links[1])]:
					st = linkMap[int(leg_links[0])][1]
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
			if int(self.legs["Leg_ID"][i]) == 1:
				legMap[pid].append([stInd, endInd])
			else:
				legMap[pid][int(self.legs["Trip_ID"][i])-1][1] = endInd
		# Debug
		# print ("Legs parsed: " + str(len(self.legs["PID"]) - ignored))
		# print ("Legs ignored: " + str(ignored))
		if ignored * 2 >= len(self.legs["PID"]) and not ("y" in str(input("***MORE THAN 50% OF THE LEGS TABLE WAS NOT USABLE. THIS IS MOST LIKELY CAUSED BY A BROKEN TABLE. CONTINUE? y/n***   ")).lower()):
			return 0
		
		#Parse input per trip
		ignored = 0
		ignored_modes = []
		for i in range(len(self.trips["PID"])):
			# debug
			# if i % 1000 == 0:
			# 	print("Parsing trips: " + str(i) + " / " + str(len(self.trips["PID"])))
			stInd = legMap[self.trips["PID"][i]][int(self.trips["Trip_ID"][i])-1][0] 
			endInd = legMap[self.trips["PID"][i]][int(self.trips["Trip_ID"][i])-1][1]
			try:
				modeFactor = Visualization.MODES.index(self.trips["realizedTripMode"][i])
			except:
				ignored+=1
				if not (self.trips["realizedTripMode"][i] in ignored_modes):
					ignored_modes.append(self.trips["realizedTripMode"][i])
				continue
			
			inds = []
			if isTAZ:
				# Get index for specific filters
				wage = popMap[self.trips["PID"][i]]
				time = (int(self.trips["End_time"][i]) + int(self.trips["Start_time"][i])) / 2
				wageFactor = max((wage-1) // Visualization.INCOME_SEP, 0)
				timeFactor = max((time-1) // Visualization.TIME_SEP, 0)
				timeIndex = timeFactor * math.ceil(200000 / Visualization.INCOME_SEP) * len(Visualization.MODES) * len(polys)
				wageIndex = wageFactor * len(Visualization.MODES) * len(polys)
				modeIndex = modeFactor * len(polys)
				relevantIndex = 0
				if useStartPoints:
					relevantIndex = stInd
				else:
					relevantIndex = endInd
				ind = int(timeIndex + wageIndex + modeIndex + relevantIndex)
				all_modeInd = int(timeIndex + wageIndex + relevantIndex)
				inds = [ind, all_modeInd]
			else:
				if useStartPoints:
					relevantIndex = stInd
				else:
					relevantIndex = endInd
				ind = modeFactor * (len(polys)+1) * len(polys) + (endInd+1) * len(polys) + relevantIndex
				all_modeInd = (endInd+1) * len(polys) + relevantIndex
				all_fromInd = modeFactor * (len(polys)+1) * len(polys) + relevantIndex
				all_allInd = relevantIndex
				inds = [ind, all_modeInd, all_fromInd, all_allInd]
			cost = (int(self.trips["fuelCost"][i]) + int(self.trips["Toll"][i])) if int(self.trips["Fare"][i]) == 0 else int(self.trips["Fare"][i])
			for o, index in enumerate(inds):
				zones["features"][index]["properties"]["total cost ($)"] += cost
				zones["features"][index]["properties"]["numVals"] += 1

		# Debug
		# print ("Trips parsed: " + str(len(self.trips["PID"]) - ignored))
		# print ("Trips ignored: " + str(ignored))
		# print ("Modes ignored: " + ", ".join(ignored_modes))
		if ignored * 2 >= len(self.legs["PID"]) and not ("y" in str(input("***MORE THAN 50% OF THE TRIPS TABLE WAS NOT USABLE. THIS IS MOST LIKELY CAUSED BY A BROKEN TABLE. CONTINUE? y/n***   ")).lower()):
			return 0


		for i, poly in enumerate(zones["features"]):
			if poly["properties"]["numVals"] > 0:
				poly["properties"]["average cost ($)"] = round(poly["properties"]["total cost ($)"] / poly["properties"]["numVals"] * 10) / 10
		
		with open (Visualization.OUTPUT_FOLDER+"/costsByZone_" + ("Start" if useStartPoints else "end") + "_" + ("TAZ" if isTAZ else "neighbors")+".json", "w") as out:
			json.dump(zones, out, allow_nan=True)
		rv.createVisual(Visualization.OUTPUT_FOLDER+"/costsByZone_" + ("Start" if useStartPoints else "end") + "_" + ("TAZ" if isTAZ else "neighbors")+".json", "../input_files/configs/costsByZone_" + ("Start" if useStartPoints else "end") + "_" + ("TAZ" if isTAZ else "neighbors")+".json", "../visualizations/temp/costsByZone_" + ("Start" if useStartPoints else "end") + "_" + ("TAZ" if isTAZ else "neighbors")+".json", "../input_files/circle_params.txt")
	def modeShareByZone(self, isTAZ):
		
		#Run as multithread
		def processNodes (**kwargs):
			'''kwargs: polys, nodes, st, end'''
			nodes = kwargs.get("nodes")
			polys = kwargs.get("polys")
			st = kwargs.get("st")
			end = kwargs.get("end")

			# Associate [nodeID:[long, lat]]
			nMap = [0 for i in range(100000)]
			for i in range(st, end):
				# nodeMap[int(n["properties"]["id"])] = n["geometry"]["coordinates"]
				pos = geo.Point([nodes["x"][i], nodes["y"][i]])
				for j, poly in enumerate(polys):
					if pos.within(poly):
						nMap[int(nodes["NodeId"][i])] = j
						break
			return nMap
		def processLegs (**kwargs):
			'''kwargs: legs, nodeMap, linkMap, stL, endL'''
			stL = kwargs.get("stL")
			endL = kwargs.get("endL")
			legs = kwargs.get("legs")
			nodeMap = kwargs.get("nodeMap")
			linkMap = kwargs.get("linkMap")
			# Associate {PID: [nodeID, nodeID, nodeID...]}
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
				
				if not (mode in Visualization.MODES):
					ignored+=1
					if not (mode in ignored_modes):
						ignored_modes.append(mode)
					continue
			
				inds = []
				if isTAZ:
					wage = popMap[pid]
					wageFactor = max((wage-1) // Visualization.INCOME_SEP, 0)
					time = min((int(endTime) + int(stTime)) / 2, 24*60*60) # Simulation has 30 hours, but we only want 24
					timeFactor = int(max((time-1) // Visualization.TIME_SEP, 0))
					for j in range(len(Visualization.MODE_GROUPS)):
						inds.append(int(j * math.ceil(24 * 60 * 60 / Visualization.TIME_SEP) * math.ceil(200000 / Visualization.INCOME_SEP) * len(polys) + timeFactor * math.ceil(200000 / Visualization.INCOME_SEP) * len(polys) + wageFactor * len(polys) + stInd))
				else:
					for j in range(len(Visualization.MODE_GROUPS)):
						inds.append(int(j * (len(polys)+1) * len(polys) + (endInd+1) * len(polys) + stInd))
						inds.append(int(j * (len(polys)+1) * len(polys) + stInd))
				threadLock.acquire()
				for index in inds:
					zones["features"][index]["properties"][mode] += 1
					if str(zones["features"][index]["properties"]["modal_group"]) == Visualization.MODE_GROUPS[Visualization.MODE_GROUP_DICT[mode]]:
						zones["features"][index]["properties"]["modal_count"] += 1
				threadLock.release()
			return [missingNum, ignored, ignored_modes]

		if os.path.exists("../output_files/missingPIDs.csv"):
			os.remove("../output_files/missingPIDs.csv")

		print ("Creating visual: modeShareByZone. IsTAZ = " + str(isTAZ))
		zones = {}
		# print ("Copying zone information..")
		if isTAZ:
			zones = copy.deepcopy(self.TAZZones)
		else:
			zones = copy.deepcopy(self.neighborZones)
		polys = []
		for i, poly in enumerate(zones["features"]):
			polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
			for j, m in enumerate(Visualization.MODES):
				poly["properties"][m] = 0
				poly["properties"][m + "_percentage"] = ""
			poly["properties"]["modal_group"] = Visualization.MODE_GROUPS[0]
			poly["properties"]["modal_count"] = 0
			poly["properties"]["modal_percentage"] = "0%"
			poly["properties"]["modal group percentage"] = 0
			poly["properties"]["ind"] = i
			if isTAZ:
				poly["properties"]["income"] = "0-" + str(Visualization.INCOME_SEP)
				poly["properties"]["time"] = "0:00:00"
			else:
				poly["properties"]["origin"] = "all"

		index = len(zones["features"])
		if isTAZ:
			# Create zones
			for M in range(len(Visualization.MODE_GROUPS)):
				for t in range(math.ceil(24 * 60 * 60 / Visualization.TIME_SEP)):
					for inc in range(math.ceil(200000 / Visualization.INCOME_SEP)):
						if inc == 0 and M == 0 and t == 0:
							continue
						for j in range(len(polys)):
							poly = zones["features"][j]
							zones["features"].append(copy.deepcopy(poly))
							zones["features"][index]["properties"]["modal_group"] = Visualization.MODE_GROUPS[M]
							zones["features"][index]["properties"]["ind"] = index
							zones["features"][index]["properties"]["time"] = "%d:%02d:%02d" %((t * Visualization.TIME_SEP) // 3600, ((t*Visualization.TIME_SEP) // 60) % 60, (t * Visualization.TIME_SEP) % 60)
							zones["features"][index]["properties"]["income"] = ( str(inc * Visualization.INCOME_SEP) if inc == 0 else str(inc * Visualization.INCOME_SEP + 1) ) + "-" + str((inc+1) * Visualization.INCOME_SEP)
							index += 1
							# if index % 1000 == 0:
							# 	print ("Creating zones: " + str(index - len(polys)) + " / " + str(math.ceil(200000 / Visualization.INCOME_SEP) * math.ceil(24 * 60 * 60 / Visualization.TIME_SEP) * len(polys) * len(Visualization.MODE_GROUPS)))
		else:
			# Create zones for filters
			for M in range(len(Visualization.MODE_GROUPS)):
				for i in range(len(polys)+1):
					if i == 0 and M == 0:
						continue
					for j in range(len(polys)):
						poly = zones["features"][j]
						zones["features"].append(copy.deepcopy(poly))
						if i == 0:
							zones["features"][index]["properties"]["origin"] = "all"
						else:
							zones["features"][index]["properties"]["origin"] = zones["features"][i-1]["properties"]["name"]
						zones["features"][index]["properties"]["modal_group"] = Visualization.MODE_GROUPS[M]
						zones["features"][index]["properties"]["ind"] = index
						index += 1
						# if index % 1000 == 0:
						# 	print ("Creating zones: " + str(index - len(polys)) + " / " + str((len(polys)+1) * len(polys) * len(Visualization.MODE_GROUPS)))
		#Create links
		linkMap = [[] for i in range(100000)]
		for i in range(len(self.links["LinkId"])):
			linkMap[int(self.links["LinkId"][i])] = [int(self.links["fromLocationID"][i]), int(self.links["toLocationID"][i])]
		
		nodeMap = [0 for i in range(100000)]
		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.nodes["NodeId"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.nodes["NodeId"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "node_thread_" + str(i), processNodes, polys=polys, nodes=self.nodes, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
			for i, num in enumerate(thread.out):
				if num > 0:
					nodeMap[i] = num
			
		popMap = {}
		for i in range(len(self.population["PID"])):
			popMap[self.population["PID"][i]] = int(self.population["income"][i])
		
		legMap = {}
		ignored = 0
		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.legs["PID"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.legs["PID"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "leg_thread_" + str(i), processLegs, legs=self.legs, nodeMap=nodeMap, linkMap=linkMap, stL=st, endL=end))
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

		# Debug
		# print ("Legs parsed: " + str(len(self.legs["PID"]) - ignored))
		# print ("Legs ignored: " + str(ignored))
		if ignored * 2 >= len(self.legs["PID"]) and not ("y" in str(input("***MORE THAN 50% OF THE LEGS TABLE WAS NOT USABLE. THIS IS MOST LIKELY CAUSED BY A BROKEN TABLE. CONTINUE? y/n***   ")).lower()):
			return 0

		#Parse input per trip
		
		ignored = 0
		ignored_modes = []
		missingNum = 0
		threadLock = threading.Lock()
		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.trips["PID"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.trips["PID"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "trip_thread_" + str(i), processTrips, trips=self.trips, popMap=popMap, legMap=legMap, stP=st, endP=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
			missingNum += thread.out[0]
			ignored += thread.out[1]
			for i, m in enumerate(thread.out[2]):
				if not (m in ignored_modes):
					ignored_modes.append(m)

		# Debug
		# print ("Trips parsed: " + str(len(self.trips["PID"]) - ignored))
		# print ("Trips ignored / broken: " + str(ignored))
		# print ("Modes ignored: " + ", ".join(ignored_modes))
		print ("%i PIDs missing from the legs table. See ../output_files/missingPIDs.csv for the full list" %(missingNum))
		if ignored * 2 >= len(self.legs["PID"]) and not ("y" in str(input("***MORE THAN 50% OF THE TRIPS TABLE WAS NOT USABLE. THIS IS MOST LIKELY CAUSED BY A BROKEN TABLE. CONTINUE? y/n***   ")).lower()):
			return 0


		for i, poly in enumerate(zones["features"]):
			totNum = 0
			for j, m in enumerate(Visualization.MODES):
				totNum += poly["properties"][m]
			if totNum == 0:
				continue
			for j, m in enumerate(Visualization.MODES):
				poly["properties"][m + "_percentage"] = "%i%%"%((poly["properties"][m] / totNum) * 100)
			poly["properties"]["modal_percentage"] = "%i%%"%((poly["properties"]["modal_count"] / totNum) * 100)
			poly["properties"]["modal group percentage"] = (poly["properties"]["modal_count"] / totNum)
		with open (Visualization.OUTPUT_FOLDER+"/modeShare_"+("TAZ" if isTAZ else "neighbors")+".json", "w") as out:
			json.dump(zones, out, allow_nan=True)
		rv.createVisual(Visualization.OUTPUT_FOLDER+"/modeShare_"+("TAZ" if isTAZ else "neighbors")+".json", "../input_files/configs/modeShare_"+("TAZ" if isTAZ else "neighbors")+".json", "../visualizations/temp/modeShare_"+("TAZ" if isTAZ else "neighbors")+".json", "../input_files/circle_params.txt")
	def speedByZone(self, isTAZ):
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
				for j, poly in enumerate(polys):
					if pos.within(poly):
						nMap[int(nodes["NodeId"][i])] = j
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
					timeFactor = int(max((hour * 3600 - 1) // Visualization.TIME_SEP, 0))
					inds.append(timeFactor * 2 * len(polys) + linkFactor * len(polys) + fromInd)
					inds.append(timeFactor * 2 * len(polys) + linkFactor * len(polys) + toInd)
				else:
					inds.append(linkFactor * len(polys) + fromInd)
					inds.append(linkFactor * len(polys) + toInd)
				threadLock.acquire()
				for index in inds:
					zones["features"][index]["properties"]["totSpeed"] += speed
					zones["features"][index]["properties"]["numVals"] += 1
				threadLock.release()
			return None

		print ("Creating visual: speedByZone. IsTAZ = " + str(isTAZ))

		zones = {}
		# print ("Copying zone information..")
		if isTAZ:
			zones = copy.deepcopy(self.TAZZones)
		else:
			zones = copy.deepcopy(self.neighborZones)
		polys = []
		for i, poly in enumerate(zones["features"]):
			polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
			poly["properties"]["totSpeed"] = 0
			poly["properties"]["numVals"] = 0
			poly["properties"]["average speed"] = 0
			poly["properties"]["ind"] = i
			poly["properties"]["linkType"] = "motorway"
			if not isTAZ:
				poly["properties"]["time"] = "0:00:00"

		index = len(polys)
		if not isTAZ:
			for t in range(math.ceil(24 * 60 * 60 / Visualization.TIME_SEP)):
				for j in range(2):
					for i in range(len(polys)):
						if t == 0 and j == 0:
							continue
						poly = zones["features"][i]
						zones["features"].append(copy.deepcopy(poly))
						zones["features"][index]["properties"]["linkType"] = "motorway" if i == 0 else "residential"
						zones["features"][index]["properties"]["ind"] = index
						zones["features"][index]["properties"]["time"] = "%d:%02d:%02d" %((t * Visualization.TIME_SEP) // 3600, ((t*Visualization.TIME_SEP) // 60) % 60, (t * Visualization.TIME_SEP) % 60)
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
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.nodes["NodeId"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.nodes["NodeId"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "node_thread_" + str(i), processNodes, polys=polys, nodes=self.nodes, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
			for i, num in enumerate(thread.out):
				if num > 0:
					nodeMap[i] = num
		
		threadLock = threading.Lock()
		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = max((i * len(speeds)) // Visualization.NUM_THREADS, 1)
			end = ((i+1) * len(speeds)) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "data_thread_" + str(i), processSpeeds, speeds=speeds, polys=polys, stP=st, endP=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
		
		for i, zone in enumerate(zones["features"]):
			if zone["properties"]["numVals"] > 0:
				zone["properties"]["average speed"] = zone["properties"]["totSpeed"] / zone["properties"]["numVals"]
		with open (Visualization.OUTPUT_FOLDER+"/speedByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "w") as out:
			json.dump(zones, out, allow_nan=True)
	def VMTByZone (self, isTAZ):
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
				for j, poly in enumerate(polys):
					if pos.within(poly):
						nMap[int(nodes["NodeId"][i])] = j
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
					timeFactor = int(max((hour * 3600 - 1) // Visualization.TIME_SEP, 0))
					inds.append(timeFactor * 2 * len(polys) + linkFactor * len(polys) + fromInd)
					inds.append(timeFactor * 2 * len(polys) + linkFactor * len(polys) + toInd)
				else:
					inds.append(linkFactor * len(polys) + fromInd)
					inds.append(linkFactor * len(polys) + toInd)
				threadLock.acquire()
				for index in inds:
					zones["features"][index]["properties"]["VMT"] += volume * length * meterToMile
				threadLock.release()
			return None

		print ("Creating visual: VMTByZone. IsTAZ = " + str(isTAZ))
		zones = {}
		# print ("Copying zone information..")
		if isTAZ:
			zones = copy.deepcopy(self.TAZZones)
		else:
			zones = copy.deepcopy(self.neighborZones)
		polys = []
		for i, poly in enumerate(zones["features"]):
			polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
			poly["properties"]["VMT"] = 0
			poly["properties"]["ind"] = i
			poly["properties"]["linkType"] = "motorway"
			if not isTAZ:
				poly["properties"]["time"] = "0:00:00"
				poly["properties"]["mode"] = Visualization.MODES[0]
				
		
		index = len(polys)
		if not isTAZ: # + per mode
			for m in range(len(Visualization.MODES)):
				for i in range(2):
					for t in range(math.ceil(24 * 60 * 60 / Visualization.TIME_SEP)):
						if m == 0 and i == 0 and t == 0:
							continue
						for j in range(len(polys)):
							poly = zones["features"][j]
							zones["features"].append(copy.deepcopy(poly))
							zones["features"][index]["properties"]["ind"] = index
							zones["features"][index]["properties"]["time"] = "%d:%02d:%02d" %((t * Visualization.TIME_SEP) // 3600, ((t*Visualization.TIME_SEP) // 60) % 60, (t * Visualization.TIME_SEP) % 60)
							zones["features"][index]["properties"]["linkType"] = ["motorway", "residential"][i]
							zones["features"][index]["properties"]["mode"] = Visualization.MODES[m]
							index += 1
							# if index % 1000 == 0:
							# 	print ("Creating zones: " + str(index - len(polys)) + " / " + str(len(Visualization.MODES) * 2 * math.ceil(24 * 60 * 60 / Visualization.TIME_SEP)))
		else:
			for j in range(len(polys)):
				poly = zones["features"][j]
				zones["features"].append(copy.deepcopy(poly))
				zones["features"][index]["properties"]["ind"] = index
				zones["features"][index]["properties"]["linkType"] = "residential"
				index += 1
				# if index % 1000 == 0:
				# 	print ("Creating zones: " + str(index - len(polys)) + " / " + str(len(polys)))
		nodeMap = [0 for i in range(100000)]
		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.nodes["NodeId"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.nodes["NodeId"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "node_thread_" + str(i), processNodes, polys=polys, nodes=self.nodes, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
			for i, num in enumerate(thread.out):
				if num > 0:
					nodeMap[i] = num
		
		threadLock = threading.Lock()
		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = max((i * len(speeds)) // Visualization.NUM_THREADS, 1)
			end = ((i+1) * len(speeds)) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "data_thread_" + str(i), processVMT, data=speeds, polys=polys, stP=st, endP=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()

		with open (Visualization.OUTPUT_FOLDER+"/VMT"+("TAZ" if isTAZ else "neighbors")+".json", "w") as out:
			json.dump(zones, out, allow_nan=True)
	def occupancyByZone(self, isTAZ):
		def processNodes (**kwargs):
			'''kwargs: polys, nodes, st, end'''
			nodes = kwargs.get("nodes")
			polys = kwargs.get("polys")
			st = kwargs.get("st")
			end = kwargs.get("end")
			# Associate [nodeID:[long,lat]]
			nMap = [0 for i in range(100000)]
			for i in range(st, end):
				# nodeMap[int(n["properties"]["id"])] = n["geometry"]["coordinates"]
				pos = geo.Point([nodes["x"][i], nodes["y"][i]])
				for j, poly in enumerate(polys):
					if pos.within(poly):
						nMap[int(nodes["NodeId"][i])] = j
						break
			return nMap
		def processPaths (**kwargs):
			'''kwargs: paths, linkMap, st, end'''
			paths = kwargs.get("paths")
			linkMap = kwargs.get("linkMap")
			st = kwargs.get("st")
			end = kwargs.get("end")
			# Associate [ind:[nodeID, nodeID, nodeID...]]
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
			# Associate [vehicleID:vehicleType]
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
			# Associate [vehicleType:capacity]
			for i in range(st, end):
				vehicleType = vehicleTypes["vehicleTypeId"][i]
				capacity = max(1, int(vehicleTypes["seatingCapacity"][i]) + int(vehicleTypes["standingRoomCapacity"][i]))
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
				occupancy = paths["numPassengers"][i] + 1
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
					timeFactor = int(max((time-1) // Visualization.TIME_SEP, 0))
					modeFactor = Visualization.MODES.index(mode)

					threadLock.acquire()
					if isTAZ:
						ind = modeFactor * math.ceil(24 * 60 * 60 / Visualization.TIME_SEP) * len(polys) + timeFactor * len(polys) + zone
						zones["features"][ind]["properties"]["totalVehicleOccupancy"] += float(occupancy) / float(capacity)
						zones["features"][ind]["properties"]["numVals"] += 1
					else:
						ind = modeFactor * math.ceil(24 * 60 * 60 / Visualization.TIME_SEP) * len(polys) + timeFactor * len(polys) + zone
						zones["features"][ind]["properties"]["totalVehicleOccupancy"] += float(occupancy) / float(capacity)
						zones["features"][ind]["properties"]["numVals"] += 1
					threadLock.release()

		print ("Creating visual: occupancyByZone. IsTAZ = " + str(isTAZ))
		zones = {}
		# print ("Copying zone information..")
		if isTAZ:
			zones = copy.deepcopy(self.TAZZones)
		else:
			zones = copy.deepcopy(self.neighborZones)
		polys = []
		for i, poly in enumerate(zones["features"]):
			polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
			poly["properties"]["totalVehicleOccupancy"] = 0.0
			poly["properties"]["numVals"] = 0
			poly["properties"]["average occupancy"] = None
			poly["properties"]["ind"] = i
			poly["properties"]["time"] = "0:00:00"
			poly["properties"]["mode"] = Visualization.MODES[0]

		index = len(zones["features"])
		# if not isTAZ:
		for m in range(len(Visualization.MODES)):
			for t in range(math.ceil(24 * 60 * 60 / Visualization.TIME_SEP)):
				if m == 0 and t == 0:
					continue
				for j in range(len(polys)):
					poly = zones["features"][j]
					zones["features"].append(copy.deepcopy(poly))
					zones["features"][index]["properties"]["time"] = "%d:%02d:%02d" %((t * Visualization.TIME_SEP) // 3600, ((t*Visualization.TIME_SEP) // 60) % 60, (t * Visualization.TIME_SEP) % 60)
					zones["features"][index]["properties"]["mode"] = Visualization.MODES[m]
					zones["features"][index]["properties"]["ind"] = index
					index += 1
					# if index % 1000 == 0:
					# 	print ("Creating zones: " + str(index) + " / " + str(len(polys) * math.ceil(24 * 60 * 60 / Visualization.TIME_SEP) * len(Visualization.MODES)))

		nodeMap = [0 for i in range(100000)]
		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.nodes["NodeId"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.nodes["NodeId"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "node_thread_" + str(i), processNodes, polys=polys, nodes=self.nodes, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
			for i, num in enumerate(thread.out):
				if num > 0:
					nodeMap[i] = num

		linkMap = [0 for i in range(100000)]
		for i in range(len(self.links["LinkId"])):
			linkMap[int(self.links["LinkId"][i])] = (self.links["fromLocationID"][i], self.links["toLocationID"][i])

		threadLock = threading.Lock()
		pathList = []
		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.paths["path"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.paths["path"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "path_thread_" + str(i), processPaths, paths=self.paths, linkMap=linkMap, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
			pathList = pathList + thread.out
		
		vehicleMap = {}
		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.vehicles["vehicle"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.vehicles["vehicle"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "vehicle_thread_" + str(i), processVehicles, vehicles=self.vehicles, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
			vehicleMap.update(thread.out)

		vehicleTypeMap = {}
		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.vehicleTypes["vehicleTypeId"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.vehicleTypes["vehicleTypeId"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "vehicleType_thread_" + str(i), processVehicleTypes, vehicleTypes=self.vehicleTypes, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
			vehicleTypeMap.update(thread.out)

		threads = []
		threadLock = threading.Lock()
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.paths["path"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.paths["path"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "data_thread_" + str(i), processData, paths=self.paths, pathList=pathList, nodeMap=nodeMap, vehicleMap=vehicleMap, vehicleTypeMap=vehicleTypeMap, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
		
		for i in range(len(zones["features"])):
			if zones["features"][i]["properties"]["numVals"] > 0:
				zones["features"][i]["properties"]["average occupancy"] = zones["features"][i]["properties"]["totalVehicleOccupancy"] / float(zones["features"][i]["properties"]["numVals"])

		with open (Visualization.OUTPUT_FOLDER+"/occupancyByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "w") as out:
			json.dump(zones, out, allow_nan=True)
		rv.createVisual(Visualization.OUTPUT_FOLDER+"/occupancyByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "../input_files/configs/occupancyByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "../visualizations/temp/occupancyByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "../input_files/circle_params.txt")
	def timeDelayByZone(self, isTAZ):
		def processNodes (**kwargs):
			'''kwargs: polys, nodes, st, end'''
			nodes = kwargs.get("nodes")
			polys = kwargs.get("polys")
			st = kwargs.get("st")
			end = kwargs.get("end")

			nMap = [0 for i in range(10000)]
			for i in range(st, end):
				pos = geo.Point([nodes["x"][i], nodes["y"][i]])
				for j, poly in enumerate(polys):
					if pos.within(poly):
						nMap[int(nodes["NodeId"][i])] = j
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
			linkMap = kwargs.get("linkMap")
			nodeMap = kwargs.get("nodeMap")
			popMap = kwargs.get("popMap")
			st = kwargs.get("st")
			end = kwargs.get("end")
			
			for i in range(st, end):
				threadLock.acquire()
				stTime = paths["departureTime"][i]
				endTime = paths["arrivalTime"][i]
				mode = paths["mode"][i]
				driver = paths["driverID"][i]
				path = paths["path"][i]
				passengers = paths["passengers"][i]
				threadLock.release()
				
				totalIncome = 0
				numPassengers = 0
				if driver in popMap:
					totalIncome = popMap[driver]
					numPassengers = len(passengers) + 1
				else:
					numPassengers = len(passengers)
				for passenger in passengers:
					if passenger:
						totalIncome += popMap[passenger]
				income = totalIncome / numPassengers
				if not pathList[i][0]:
					continue
				origin = nodeMap[pathList[i][0]]
				destination = nodeMap[pathList[i][-1]]
				expectedTime = 0
				# TO NOTE: speed is in mph, but length is in meters
				for link in path:
					expectedTime += (0.44704 * linkMap[int(link)][3]) / linkMap[int(link)][2]
				totalTime = endTime - stTime

				time = min((int(endTime) + int(stTime)) / 2, 24*60*60) # The viz is made for a 24h simulation, but it's actually a 30 hour simulation because that makes sense
				timeFactor = int(max((time-1) // Visualization.TIME_SEP, 0))
				modeFactor = Visualization.MODES.index(mode)
				incomeFactor = max((income-1) // Visualization.INCOME_SEP, 0)
				inds = []
				if isTAZ:
					mFactor = int(modeFactor * math.ceil(24 * 60 * 60 / Visualization.TIME_SEP) * math.ceil(200000 / Visualization.INCOME_SEP) * len(polys))
					tFactor = int(timeFactor * math.ceil(200000 / Visualization.INCOME_SEP) * len(polys))
					iFactor = int(incomeFactor * len(polys))
					inds.append(mFactor + tFactor + iFactor + origin)
					inds.append(mFactor + tFactor + iFactor + destination)
				else:
					inds.append(modeFactor * (len(polys)+1) * len(polys) + origin * len(polys) + origin)
					inds.append(modeFactor * (len(polys)+1) * len(polys) + origin * len(polys) + destination)

				threadLock.acquire()
				for ind in inds:
					zones["features"][ind]["properties"]["time delay (s)"] = totalTime - expectedTime
				threadLock.release()

		print ("Creating visual: timeDelayByZone. IsTAZ = " + str(isTAZ))
		zones = {}
		# print ("Copying zone information...")
		if isTAZ:
			zones = copy.deepcopy(self.TAZZones)
		else:
			zones = copy.deepcopy(self.neighborZones)
		polys = []
		for i, poly in enumerate(zones["features"]):
			polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
			poly["properties"]["time delay (s)"] = None
			poly["properties"]["mode"] = Visualization.MODES[0]
			poly["properties"]["ind"] = i
			if isTAZ:
				poly["properties"]["time"] = "0:00:00"
				poly["properties"]["income"] = "0-" + str(Visualization.INCOME_SEP)
			else:
				poly["properties"]["origin"] = "all"

		index = len(zones["features"])
		if isTAZ:
			for m in range(len(Visualization.MODES)):
				for t in range(math.ceil(24 * 60 * 60 / Visualization.TIME_SEP)):
					for inc in range(math.ceil(200000 / Visualization.INCOME_SEP)):
						if m == 0 and t == 0 and inc == 0:
							continue
						for j in range(len(polys)):
							poly = zones["features"][j]
							zones["features"].append(copy.deepcopy(poly))
							zones["features"][index]["properties"]["time"] = "%d:%02d:%02d" %((t * Visualization.TIME_SEP) // 3600, ((t*Visualization.TIME_SEP) // 60) % 60, (t * Visualization.TIME_SEP) % 60)
							zones["features"][index]["properties"]["mode"] = Visualization.MODES[m]
							zones["features"][index]["properties"]["income"] = ( str(inc * Visualization.INCOME_SEP) if inc == 0 else str(inc * Visualization.INCOME_SEP + 1) ) + "-" + str((inc+1) * Visualization.INCOME_SEP)
							zones["features"][index]["properties"]["ind"] = index
							index += 1
							# if index % 1000 == 0:
							# 	print ("Creating zones: " + str(index) + " / " + str(len(polys) * math.ceil(24 * 60 * 60 / Visualization.TIME_SEP) * len(Visualization.MODES) * math.ceil(200000 / Visualization.INCOME_SEP)))
		else:
			for m in range(len(Visualization.MODES)):
				for i in range(len(polys)+1):
					if m == 0 and i == 0:
						continue
					for j in range(len(polys)+1):
						poly = zones["features"][j]
						zones["features"].append(copy.deepcopy(poly))
						zones["features"][index]["properties"]["mode"] = Visualization.MODES[m]
						if i == 0:
							zones["features"][index]["properties"]["origin"] = "all"
						else:
							zones["features"][index]["properties"]["origin"] = zones["features"][i-1]["properties"]["name"]
						zones["features"][index]["properties"]["ind"] = index
						index += 1
						# if index % 1000 == 0:
						# 	print ("Creating zones: " + str(index) + " / " + str(len(polys) * len(Visualization.MODES) * len(polys)+1))

		nodeMap = [0 for i in range(10000)]
		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.nodes["NodeId"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.nodes["NodeId"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "node_thread_" + str(i), processNodes, polys=polys, nodes=self.nodes, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
			for i, num in enumerate(thread.out):
				if num > 0:
					nodeMap[i] = num
		popMap = {}
		for i in range(len(self.population["PID"])):
			popMap[self.population["PID"][i]] = int(self.population["income"][i])

		linkMap = [0 for i in range(100000)]
		for i in range(len(self.links["LinkId"])):
			linkMap[int(self.links["LinkId"][i])] = (self.links["fromLocationID"][i], self.links["toLocationID"][i], self.links["freeSpeed"][i], self.links["length"][i])

		threadLock = threading.Lock()
		pathList = []
		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.paths["path"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.paths["path"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "path_thread_" + str(i), processPaths, paths=self.paths, linkMap=linkMap, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
			pathList = pathList + thread.out

		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.paths["path"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.paths["path"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "processing_thread_" + str(i), processData, paths=self.paths, linkMap=linkMap, popMap=popMap, pathList=pathList, nodeMap=nodeMap, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
		
		with open (Visualization.OUTPUT_FOLDER+"/timeDelayByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "w") as out:
			json.dump(zones, out, allow_nan=True)
		rv.createVisual(Visualization.OUTPUT_FOLDER+"/timeDelayByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "../input_files/configs/timeDelayByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "../visualizations/temp/timeDelayByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "../input_files/circle_params.txt")
	def travelDistanceByZone(self, isTAZ):
		def processNodes (**kwargs):
			'''kwargs: polys, nodes, st, end'''
			nodes = kwargs.get("nodes")
			polys = kwargs.get("polys")
			st = kwargs.get("st")
			end = kwargs.get("end")

			nMap = [0 for i in range(10000)]
			for i in range(st, end):
				pos = geo.Point([nodes["x"][i], nodes["y"][i]])
				for j, poly in enumerate(polys):
					if pos.within(poly):
						nMap[int(nodes["NodeId"][i])] = j
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
			linkMap = kwargs.get("linkMap")
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
				driver = paths["driverID"][i]
				passengers = paths["passengers"][i]
				threadLock.release()

				totalIncome = 0
				if driver in popMap:
					totalIncome = popMap[driver]
					numPassengers = len(passengers) + 1
				else:
					# Ridehail drivers are tracked weirdly, so we'll pretend they don't exist
					numPassengers = len(passengers)
				for passenger in passengers:
					if passenger:
						totalIncome += popMap[passenger]
				income = totalIncome / numPassengers
				if not pathList[i][0]:
					continue
				origin = nodeMap[pathList[i][0]]
				destination = nodeMap[pathList[i][-1]]
				totalDistance = 0
				for link in path:
					totalDistance += linkMap[int(link)][3]

				time = min((int(endTime) + int(stTime)) / 2, 24*60*60) # The viz is made for a 24h simulation, but it's actually a 30 hour simulation because that makes sense
				timeFactor = int(max((time-1) // Visualization.TIME_SEP, 0))
				modeFactor = Visualization.MODES.index(mode)
				incomeFactor = max((income-1) // Visualization.INCOME_SEP, 0)
				inds = []
				if isTAZ:
					mFactor = int(modeFactor * math.ceil(24 * 60 * 60 / Visualization.TIME_SEP) * math.ceil(200000 / Visualization.INCOME_SEP) * len(polys))
					tFactor = int(timeFactor * math.ceil(200000 / Visualization.INCOME_SEP) * len(polys))
					iFactor = int(incomeFactor * len(polys))
					inds.append(mFactor + tFactor + iFactor + origin)
					inds.append(mFactor + tFactor + iFactor + destination)
				else:
					inds.append(modeFactor * (len(polys)+1) * len(polys) + origin * len(polys) + origin)
					inds.append(modeFactor * (len(polys)+1) * len(polys) + origin * len(polys) + destination)

				threadLock.acquire()
				for ind in inds:
					zones["features"][ind]["properties"]["total distance (m)"] = totalDistance
				threadLock.release()

		print ("Creating visual: totalDistance. IsTAZ = " + str(isTAZ))
		zones = {}
		# print ("Copying zone information...")
		if isTAZ:
			zones = copy.deepcopy(self.TAZZones)
		else:
			zones = copy.deepcopy(self.neighborZones)
		polys = []
		for i, poly in enumerate(zones["features"]):
			polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
			poly["properties"]["total distance (m)"] = None
			poly["properties"]["mode"] = Visualization.MODES[0]
			poly["properties"]["ind"] = i
			if isTAZ:
				poly["properties"]["time"] = "0:00:00"
				poly["properties"]["income"] = "0-" + str(Visualization.INCOME_SEP)
			else:
				poly["properties"]["origin"] = "all"

		index = len(zones["features"])
		if isTAZ:
			for m in range(len(Visualization.MODES)):
				for t in range(math.ceil(24 * 60 * 60 / Visualization.TIME_SEP)):
					for inc in range(math.ceil(200000 / Visualization.INCOME_SEP)):
						if m == 0 and t == 0 and inc == 0:
							continue
						for j in range(len(polys)):
							poly = zones["features"][j]
							zones["features"].append(copy.deepcopy(poly))
							zones["features"][index]["properties"]["time"] = "%d:%02d:%02d" %((t * Visualization.TIME_SEP) // 3600, ((t*Visualization.TIME_SEP) // 60) % 60, (t * Visualization.TIME_SEP) % 60)
							zones["features"][index]["properties"]["mode"] = Visualization.MODES[m]
							zones["features"][index]["properties"]["income"] = ( str(inc * Visualization.INCOME_SEP) if inc == 0 else str(inc * Visualization.INCOME_SEP + 1) ) + "-" + str((inc+1) * Visualization.INCOME_SEP)
							zones["features"][index]["properties"]["ind"] = index
							index += 1
							# if index % 1000 == 0:
							# 	print ("Creating zones: " + str(index) + " / " + str(len(polys) * math.ceil(24 * 60 * 60 / Visualization.TIME_SEP) * len(Visualization.MODES) * math.ceil(200000 / Visualization.INCOME_SEP)))
		else:
			for m in range(len(Visualization.MODES)):
				for i in range(len(polys)+1):
					if m == 0 and i == 0:
						continue
					for j in range(len(polys)+1):
						poly = zones["features"][j]
						zones["features"].append(copy.deepcopy(poly))
						zones["features"][index]["properties"]["mode"] = Visualization.MODES[m]
						if i == 0:
							zones["features"][index]["properties"]["origin"] = "all"
						else:
							zones["features"][index]["properties"]["origin"] = zones["features"][i-1]["properties"]["name"]
						zones["features"][index]["properties"]["ind"] = index
						index += 1
						# if index % 1000 == 0:
						# 	print ("Creating zones: " + str(index) + " / " + str(len(polys) * len(Visualization.MODES) * len(polys)+1))

		nodeMap = [0 for i in range(10000)]
		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.nodes["NodeId"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.nodes["NodeId"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "node_thread_" + str(i), processNodes, polys=polys, nodes=self.nodes, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
			for i, num in enumerate(thread.out):
				if num > 0:
					nodeMap[i] = num
		popMap = {}
		for i in range(len(self.population["PID"])):
			popMap[self.population["PID"][i]] = int(self.population["income"][i])

		linkMap = [0 for i in range(100000)]
		for i in range(len(self.links["LinkId"])):
			linkMap[int(self.links["LinkId"][i])] = (self.links["fromLocationID"][i], self.links["toLocationID"][i], self.links["freeSpeed"][i], self.links["length"][i])

		threadLock = threading.Lock()
		pathList = []
		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.paths["path"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.paths["path"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "path_thread_" + str(i), processPaths, paths=self.paths, linkMap=linkMap, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
			pathList = pathList + thread.out

		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.paths["path"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.paths["path"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "processing_thread_" + str(i), processData, paths=self.paths, linkMap=linkMap, popMap=popMap, pathList=pathList, nodeMap=nodeMap, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
		
		with open (Visualization.OUTPUT_FOLDER+"/totalDistance_"+("TAZ" if isTAZ else "neighbors")+".json", "w") as out:
			json.dump(zones, out, allow_nan=True)
		rv.createVisual(Visualization.OUTPUT_FOLDER+"/totalDistance_"+("TAZ" if isTAZ else "neighbors")+".json", "../input_files/configs/totalDistance_"+("TAZ" if isTAZ else "neighbors")+".json", "../visualizations/temp/totalDistance_"+("TAZ" if isTAZ else "neighbors")+".json", "../input_files/circle_params.txt")
	def tripDensityByZone(self, isTAZ):
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
				for j, poly in enumerate(polys):
					if pos.within(poly):
						nMap[int(nodes["NodeId"][i])] = j
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
				threadLock.acquire() # It seems like pandas dataframe (maybe numpy array?) don't support multi-threaded processes
				pid = legs["PID"][i]
				leg_links = legs["LinkId"][i]
				legID = legs["Leg_ID"][i]
				tripId = int(legs["Trip_ID"][i])
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
				if tripId >= len(legMap[pid]):
					legMap[pid].append([stInd, endInd])
				else:
					legMap[pid][tripId-1][1] = endInd
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
				stInd = int(legMap[str(pid)][int(tid)-1][0])
				endInd = int(legMap[str(pid)][int(tid)-1][1])
				time = min((stTime + endTime) // 2, 24 * 60 * 60)
				timeFactor = int(max((time-1) // Visualization.TIME_SEP, 0))
				if isTAZ:
					originFactor = stInd * len(polys)
					startZoneInd = originFactor + stInd
					endZoneInd = originFactor + endInd
					threadLock.acquire()
					zones["features"][stInd]["properties"]["Paths starting here"] += 1 # Account for "all"
					zones["features"][endInd]["properties"]["Paths ending here"] += 1
					zones["features"][stInd]["properties"]["Total paths"] += 1
					zones["features"][endInd]["properties"]["Total paths"] += 1
					threadLock.release()
				else:
					startZoneInd = timeFactor * len(polys) + stInd
					endZoneInd = timeFactor * len(polys) + endInd
				threadLock.acquire()
				zones["features"][startZoneInd]["properties"]["Paths starting here"] += 1
				zones["features"][endZoneInd]["properties"]["Paths ending here"] += 1
				zones["features"][startZoneInd]["properties"]["Total paths"] += 1
				zones["features"][endZoneInd]["properties"]["Total paths"] += 1
				threadLock.release()
			return None
				
		threadLock = threading.Lock()

		print ("Creating visual: tripDensityByZone. IsTAZ = " + str(isTAZ))
		zones = {}
		# print ("Copying zone information..")
		if isTAZ:
			zones = copy.deepcopy(self.TAZZones)
		else:
			zones = copy.deepcopy(self.neighborZones)
		polys = []
		for i, poly in enumerate(zones["features"]):
			polys.append(geo.Polygon(poly["geometry"]["coordinates"][0]))
			poly["properties"]["Paths starting here"] = 0
			poly["properties"]["Paths ending here"] = 0
			poly["properties"]["Total paths"] = 0
			poly["properties"]["name"] = poly["properties"]["TAZCE10"]
			poly["properties"]["ind"] = i
			if isTAZ:
				poly["properties"]["origin"] = "all"
			else:
				poly["properties"]["time"] = "0:00:00"

		index = len(zones["features"])
		if isTAZ:
			for origin in range(1, len(polys)+1):
				for destination in range(len(polys)):
					poly = zones["features"][destination]
					zones["features"].append(copy.deepcopy(poly))
					zones["features"][index]["properties"]["ind"] = index
					zones["features"][index]["properties"]["origin"] = str(int(zones["features"][origin-1]["properties"]["name"]))
					index += 1
					# if index % 1000 == 0:
					# 	print ("Creating zones: " + str(index - len(polys)) + " / " + str(math.ceil(24 * 60 * 60 / Visualization.TIME_SEP) * len(polys)))
		else:
			for t in range(math.ceil(24 * 60 * 60 / Visualization.TIME_SEP)):
				if t == 0:
					continue
				for j in range(len(polys)):
					poly = zones["features"][j]
					zones["features"].append(copy.deepcopy(poly))
					zones["features"][index]["properties"]["ind"] = index
					zones["features"][index]["properties"]["time"] = "%d:%02d:%02d" %((t * Visualization.TIME_SEP) // 3600, ((t*Visualization.TIME_SEP) // 60) % 60, (t * Visualization.TIME_SEP) % 60)
					index += 1
					# if index % 1000 == 0:
					# 	print ("Creating zones: " + str(index - len(polys)) + " / " + str(math.ceil(24 * 60 * 60 / Visualization.TIME_SEP) * len(polys)))
		
		linkMap = [[] for i in range(100000)]
		for i in range(len(self.links["LinkId"])):
			linkMap[int(self.links["LinkId"][i])] = [int(self.links["fromLocationID"][i]), int(self.links["toLocationID"][i])]
		
		nodeMap = [0 for i in range(100000)]
		threads = []
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.nodes["NodeId"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.nodes["NodeId"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "node_thread_" + str(i), processNodes, polys=polys, nodes=self.nodes, st=st, end=end))
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
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.legs["PID"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.legs["PID"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "leg_thread_" + str(i), processLegs, legs=self.legs, nodeMap=nodeMap, linkMap=linkMap, stL=st, endL=end))
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
		for i in range(Visualization.NUM_THREADS):
			st = (i * len(self.trips["PID"])) // Visualization.NUM_THREADS
			end = ((i+1) * len(self.trips["PID"])) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "trip_thread_" + str(i), processTrips, trips=self.trips, legMap=legMap, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
		with open (Visualization.OUTPUT_FOLDER+"/tripDensityByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "w") as out:
			json.dump(zones, out, allow_nan=True)
		rv.createVisual(Visualization.OUTPUT_FOLDER+"/tripDensityByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "../input_files/configs/tripDensityByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "../visualizations/temp/tripDensityByZone_"+("TAZ" if isTAZ else "neighbors")+".json", "../input_files/circle_params.txt")
	def heatMap(self):
		def processLinks ():
			linkMap = {}
			for i in range(len(self.links["LinkId"])):
				linkId = str(int(self.links["LinkId"][i]))
				fromLoc = [self.links["fromLocationX"][i], self.links["fromLocationY"][i]]
				toLoc = [self.links["toLocationX"][i], self.links["toLocationY"][i]]
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
			return out

		threadLock = threading.Lock()

		print ("Creating visual: heatMap")
		linkMap = processLinks()
		out = []
		out.append("Latitude,Longitude,Time")
		threads = []
		for i in range(Visualization.NUM_THREADS):
			size = len(self.paths["path"])
			st = (i * size) // Visualization.NUM_THREADS
			end = ((i+1) * size) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "path_thread_" + str(i), processPaths, paths=self.paths, linkMap=linkMap, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
			out += thread.out
			
		with open (Visualization.OUTPUT_FOLDER+"/heatMap.csv", "w") as outFile:
			for line in out:
				outFile.write(line + "\n")
	def speedByLink(self): #TODO: make updated version
		global links, speeds
	def tripsByTime(self):
		def processLinks():
			linkMap = {}
			for i in range(len(self.links)):
				threadLock.acquire()
				linkId = str(int(self.links["LinkId"][i]))
				fromLoc = [self.links["fromLocationX"][i], self.links["fromLocationY"][i]]
				toLoc = [self.links["toLocationX"][i], self.links["toLocationY"][i]]
				threadLock.release()
				linkMap[str(linkId)] = [fromLoc, toLoc]
			return linkMap
		def processLegs (**kwargs):
			'''kwargs: legs, nodeMap, linkMap, stL, endL'''
			stL = kwargs.get("stL")
			endL = kwargs.get("endL")
			legs = kwargs.get("legs")
			linkMap = kwargs.get("linkMap")
			legMap = {}
			ignored = 0
			for i in range(stL, endL):
				threadLock.acquire()
				pid = legs["PID"][i]
				leg_links = legs["LinkId"][i]
				legID = legs["Leg_ID"][i]
				tripNum = legs["Trip_ID"][i]
				threadLock.release()
				if not (pid in list(legMap.keys())):
					legMap[pid] = []
				if math.isnan(leg_links[0]):
					continue
				link = str(int(leg_links[0]))
				if int(tripNum) > len(legMap[pid]):
					legMap[pid].append([linkMap[link][0], linkMap[link][1]])
				else:
					legMap[pid][tripNum-1][1] = linkMap[link][1]
			return legMap
		def processTrips (**kwargs):
			'''kwargs: trips, linkMap, legMap, stP, endP'''
			trips = kwargs.get("trips")
			linkMap = kwargs.get("linkMap")
			legMap = kwargs.get("legMap")
			stP = kwargs.get("st")
			endP = kwargs.get("end")
			missingNum = 0
			ignored = 0
			ignored_modes = []
			out = []
			for i in range(stP, endP):
			# for i in range(1):
				# if i % 1000 == 0:
				# 	print("Parsing trips: " + str(i) + " / " + str(len(trips["PID"])))
				stInd = 0
				endInd = 0
				pid = tid = stTime = endTime = mode = ""
				threadLock.acquire()
				pid = trips["PID"][i]
				tid = trips["Trip_ID"][i]
				stTime = trips["Start_time"][i]
				endTime = trips["End_time"][i]
				mode = trips["realizedTripMode"][i]
				threadLock.release()
				try:
					stInd = legMap[str(pid)][int(tid)-1][0]
					endInd = legMap[str(pid)][int(tid)-1][1]
				except:
					threadLock.acquire()
					with open("../output_files/missingPIDs.csv", "a") as missing:
						missing.write(pid + ",")
					threadLock.release()
					missingNum += 1
					ignored += 1
					continue
				t = stTime / Visualization.TIME_SEP
				timeTaken = endTime - stTime
				formatTime = "%d:%02d:%02d" %((t * Visualization.TIME_SEP) // 3600, ((t*Visualization.TIME_SEP) // 60) % 60, (t * Visualization.TIME_SEP) % 60)
				out.append(','.join([str(stInd[0]), str(stInd[1]), str(endInd[0]), str(endInd[1]), str(mode), formatTime, str(timeTaken)]))
			return [out, missingNum]
		
		threadLock = threading.Lock()

		print("Creating visual: tripsArcs")
		out = []
		out.append("StartLat,StartLong,EndLat,EndLong,Mode,Start Time,Time taken")
		linkMap = processLinks()
		legMap = {}
		threads = []
		for i in range(Visualization.NUM_THREADS):
			size = len(self.legs["PID"])
			st = (i * size) // Visualization.NUM_THREADS
			end = ((i+1) * size) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "leg_thread_" + str(i), processLegs, legs=self.legs, linkMap=linkMap, stL=st, endL=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
			legMap.update(thread.out)

		threads = []
		missing = 0
		for i in range(Visualization.NUM_THREADS):
			size = len(self.trips["PID"])
			st = (i * size) // Visualization.NUM_THREADS
			end = ((i+1) * size) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "trip_thread_" + str(i), processTrips, linkMap=linkMap, legMap=legMap, trips=self.trips, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
			out = out + thread.out[0]
			missing += thread.out[1]
		print ("%d PIDs missing in the leg map. Find the list of missing PIDs at ../output_files/missingPIDs.csv" %(missing))
		with open (Visualization.OUTPUT_FOLDER+"/tripsByTime.csv", "w") as outFile:
			for line in out:
				outFile.write(line + "\n")
	def followPeople(self):
		def processActivities(**kwargs):
			'''kwargs: activities, st, end'''
			activities = kwargs["activities"]
			st = kwargs["st"]
			end = kwargs["end"]
			actMap = {}
			for i in range(st, end):
				threadLock.acquire()
				pid = str(activities["PID"][i])
				actNum = activities["ActNum"][i]
				try:
					linkId = str(int(activities["LinkId"][i]))
				except:
					linkId = str(activities["LinkId"][i]) #Case: NaN
				threadLock.release()
				if not (pid in actMap):
					actMap[pid] = {}
				actMap[pid][actNum] = linkId
			return actMap #ouput: {PID: {actID: linkID}}
		def processLinks():
			linkMap = {}
			for i in range(len(self.links)):
				threadLock.acquire()
				linkId = str(int(self.links["LinkId"][i]))
				fromLoc = [self.links["fromLocationX"][i], self.links["fromLocationY"][i]]
				toLoc = [self.links["toLocationX"][i], self.links["toLocationY"][i]]
				threadLock.release()
				linkMap[str(linkId)] = [fromLoc, toLoc]
			return linkMap

		'''
		Plan:
		out[PID][time // 600 (every 10 mins)] = trip. Orig + dest might be the same if person stays in place.
		'''
		
		print("Creating visual: followPeople")
		out = {}

		threadLock = threading.Lock()
		tempMap = {}
		for i in range(len(self.trips["PID"])):
			pid = self.trips["PID"][i]
			if pid in tempMap:
				tempMap[pid] += 1
			else:
				tempMap[pid] = 1
		followedPIDs = []
		# Create output array. Currently just taking the first 100 PIDs. Could maybe later use 100 PIDs with the most trips? That's what tempMap is for, after all
		for key in tempMap.keys():
			if len(followedPIDs) < 100:
				followedPIDs.append(key)
				out[key] = [[] for i in range(24*6)]
		
		linkMap = processLinks()
		actMap = {}
		threads = []
		for i in range(Visualization.NUM_THREADS):
			size = len(self.activities["PID"])
			st = (i * size) // Visualization.NUM_THREADS
			end = ((i+1) * size) // Visualization.NUM_THREADS
			threads.append(pt.processingThread(i, "activity_thread_" + str(i), processActivities, activities=self.activities, st=st, end=end))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
			actMap.update(thread.out)

		for i in range(len(self.activities["PID"])):
			pid = self.activities['PID'][i]
			if not (pid in followedPIDs):
				continue
			stTime = self.trips["Start_time"][i]
			endTime = self.trips["End_time"][i]
			orgAct = self.trips["OriginAct"][i]
			destinationAct = self.trips["DestinationAct"][i]
			formattedStTime = "%d:%02d:%02d" %(stTime // 3600, (stTime // 60) % 60, stTime % 60)
			formattedEndTime = "%d:%02d:%02d" %(endTime // 3600, (endTime // 60) % 60, endTime % 60)
			outputLineSt = [str(pid), 
							str(linkMap[actMap[pid][orgAct]][0][0]), 
							str(linkMap[actMap[pid][orgAct]][0][1]), 
							formattedStTime]
			outputLineEnd = [str(pid),
							str(linkMap[actMap[pid][destinationAct]][1][0]),
							str(linkMap[actMap[pid][destinationAct]][1][1]), 
							str(formattedEndTime)]
			out[pid][int(min(stTime // 600, 24*6-1))] = outputLineSt
			out[pid][int(min(endTime // 600, 24*6-1))] = outputLineEnd
		with open (Visualization.OUTPUT_FOLDER+"/followPerson.csv", "w") as outFile:
			outFile.write("PID,Latitude,Longitude,time\n")
			for time in out:
				for person in out[time]:
					if len(person) > 0:
						outFile.write(",".join(person) + "\n") 


viz = Visualization("01729e42-41cd-11eb-94f5-9801a798306b")


# While this makes one individual visualization much slower, it means that I can run all of them much faster (each can be disabled if you don't want to waste time grabbing useless tables)
viz.loadTables()
viz.travelTimesByZone(True)
viz.costsByZone(True, False)
viz.costsByZone(True, True)
viz.modeShareByZone(True)
viz.occupancyByZone(True)
viz.timeDelayByZone(True)
viz.tripsByTime()
viz.followPeople()
viz.tripDensityByZone(True)
viz.heatMap()
viz.travelDistanceByZone(True)
# speedByZone(True)
# VMTByZone(True)