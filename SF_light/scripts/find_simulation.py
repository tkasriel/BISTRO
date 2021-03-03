import os, sys
sys.path.append(os.path.abspath("/Users/git/BISTRO_Dashboard/BISTRO_Dashboard"))

from db_loader import BistroDB
import numpy as np
import pandas as pd

SCENARIO = "sioux_faux-15k"

database = BistroDB('bistro', 'bistroclt', 'client', '52.53.200.197')
simul_ids = database.get_simul_by_scenario(SCENARIO)
with open ("simul_ids.txt", "w") as fileOut:
    fileOut.write("simul_id, name, time, tag\n")
    for i in range(len(simul_ids["name"])):
        sim_id = simul_ids["simulation_id"][i]
        name = simul_ids["name"][i]
        datetime = simul_ids["datetime"][i]
        tag = simul_ids["tag"][i]
        fileOut.write ("%s, %s, %s, %s\n"%(sim_id, name, datetime, tag))