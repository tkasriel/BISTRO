'''
INPUTS:
    link_info.csv: linkID, length, traversals, VMT, capacity
    link_stats.csv: coords, hour, volume, travelTime
    output: linkID, nodeStart, nodeEnd, hour, length, freespeed, capacity, stat=1000(not used), volume=AVG(not used), traveltime=length/speed, speed
    I was hoping to avoid the DB, but it looks like it's unavoidable
'''
import os, sys

sys.path.append(os.path.abspath("/Users/git/BISTRO_Dashboard/BISTRO_Dashboard"))
from db_loader import BistroDB

LINK_INFO = "../input_files/link_info.csv" #Note: file is .../competition/link_stats.csv
LINK_STATS = "../input_files/link_stats.csv" #Note: file is .../competition/viz/link_stats.csvs
SIMUL_ID = "44402ee0-7cb8-11ea-a911-063f0fd82f9f" #Simulation ID doesn't matter so long as it's from the correct scenario

def loadDB (simulation_id):
	database = BistroDB('bistro', 'bistroclt', 'client', '52.53.200.197')
	simulation_id = [simulation_id]
	scenario = database.get_scenario(simulation_id)
	return (database, scenario, simulation_id)
def loadNodes (db, scenario):
	print("Loading nodes...")
	nodes = db.load_nodes(scenario)
	print("nodes loaded")
	return nodes
def loadLinks(db, scenario):
	print("Loading links...")
	links = db.load_links(scenario)
	print("links loaded")
	return links
def readCSV(file):
    lines = file.readlines()
    out = {}
    colNames = lines[0].split(',')
    for i in range(len(colNames)):
        out[colNames[i]] = []
    for i in range (1, len(lines)):
        line = lines[i].split(',')
        for j in range(len(line)):
            out[colNames[j]].append(line[j])
    return out

(database, scenario, simulation_id) = loadDB(SIMUL_ID)
nodes = loadNodes(database, scenario)
links = loadLinks(database, scenario)


with open(LINK_INFO, "r") as file:
    linkInfo = readCSV(file)
with open(LINK_STATS, "r") as file:
    linkStats = readCSV(file)

def processNodes(nodes):
    nodeMap = {}
    for i in range(len(nodes["NodeId"])):
        x = nodes["x"][i]
        y = nodes["y"][i]
        nodeID = nodes["NodeId"][i]
        if not(x in nodeMap.keys()):
            nodeMap[x] = {}
        nodeMap[x][y] = nodeID
    return nodeMap
def processLinks(links):
    linkMap = {}
    for i in range(len(links["LinkId"])):
        fromX = links["fromLocationX"][i]
        fromY = links["fromLocationY"][i]
        toX = links["toLocationX"][i]
        toY = links["toLocationY"][i]
        linkID = links["LinkId"][i]
        if not (fromX in linkMap.keys()):
            linkMap[fromX] = {}
        if not (fromY in linkMap[fromX].keys()):
            linkMap[fromX][fromY] = {}
        if not (toX in linkMap[fromX][fromY].keys()):
            linkMap[fromX][fromY][toX] = {}
        linkMap[fromX][fromY][toX][toY] = linkID
    return linkMap
out = {}
cols = "link,from,to,hour,length,freespeed,capacity,stat,volume,traveltime,speed".split(",")
for col in cols:
    out[col] = []

nodeMap = processNodes(nodes)
linkMap = processLinks(links)
idMap = {}
index = 0
for t in range (24):
    for n in range(len(links["LinkId"])):
        idMap[str(links["LinkId"][n])] = index
        out["link"].append(links["LinkId"][n])
        out["from"].append(links["fromLocationID"][n])
        out["to"].append(links["toLocationID"][n])
        out["hour"].append(t+1)
        out["length"].append(links["length"][n])
        out["freespeed"].append(links["freeSpeed"][n])
        out["capacity"].append(0)
        out["stat"].append("AVG")
        out["volume"].append(0.0)
        out["traveltime"].append(0)
        out["speed"].append(24.99997930277778)
        index += 1
with open ("text.txt", "w") as fileOut:
    for i in range(len(nodes["NodeId"])):
        fileOut.write (str(nodes["NodeId"][i]) + "\t" + str(nodes["x"][i]) + "\t" + str(nodes["y"][i]) + "\n")
# print (linkMap)
for i in range(len(linkInfo["linkId"])):
    linkID = linkInfo["linkId"][i]
    capacity = linkInfo["linkId"][i]
    index = idMap[str(linkID)]
    out["capacity"][index] = capacity
for i in range(len(linkStats["fromX"])):
    fromX = linkStats["fromX"][i]
    fromY = linkStats["fromY"][i]
    toX = linkStats["toX"][i]
    toY = linkStats["toY"][i]
    fromID = nodeMap[fromX][fromY]
    toID = nodeMap[toX][toY]
