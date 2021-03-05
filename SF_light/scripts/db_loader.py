import configparser
import mysql.connector
from mysql.connector import Error, errorcode

import pandas as pd


def parse_credential(db_profile):
    config = configparser.ConfigParser()
    config.read(db_profile)
    db_login = config['DB_LOGIN']
    return (db_login['DATABASE_NAME'], db_login['DATABASE_USER_NAME'],
            db_login['DATABASE_KEY'], db_login['DATABASE_HOST'])


class BistroDB(object):

    db_name = None
    user_name = None
    db_key = None

    connection = None
    cursor = None

    def __init__(self, db_name, user_name, db_key, host='localhost'):

        self.db_name = db_name
        self.user_name = user_name
        self.db_key = db_key
        self.host = host

        self.connection = self.connect_to_db(self.host, self.user_name, self.db_key)
        self.cursor = self.get_cursor()

    def __del__(self):
        if self.connection and (self.connection.is_connected()):
            self.connection.close()
            print("Connection to DB {} closed".format(self.db_name))

    @staticmethod
    def connect_to_db(host, user_name, db_key):
        """
        Takes in the Database name, user name and password return connection to the database.
        If input invalid or connection has problem, return None

        """

        if not user_name or not db_key:
            print("You have not set user_name or db_key for BistroDB")
            return None

        try:
            connection = mysql.connector.connect(
                host=host, user=user_name, password=db_key
            )

            if connection.is_connected():
                print("Connected to DB at {}".format(host))
                return connection
            else:
                print("Not able to connect DB")
                return None

        except Error as e:
            print("[DB ERROR] while connection to DB", e)
            return None

    def get_cursor(self):
        """
        return the cursor from connection that's using the db_name database
        """
        if not self.db_name:
            print("You have not set db_name for BistroDB")
            return 

        cursor = self.connection.cursor()

        cursor.execute("USE {}".format(self.db_name))
        return cursor

    def get_table(self, table_name, cols=None, condition=''):
        """refer to the bistro_dbschema.py for detail columns definition"""
        if cols is None:
            select_col = '*'
        else:
            select_col = ', '.join(cols)
        self.cursor.execute(
            """SELECT {} FROM {} {}""".format(select_col, table_name, condition)
        )
        return self.cursor.fetchall()

    def query(self, q):
        """sent customized query to database"""
        self.cursor.execute(q)
        return self.cursor.fetchall()

    @staticmethod
    def binary_ids(simulation_ids):
        return ','.join(
            ["UUID_TO_BIN('{}')".format(s_id) for s_id in simulation_ids])

    def get_scenario(self, simulation_id):
        scenario = self.query("""
            SELECT simulationrun.scenario
            FROM simulationrun
            WHERE simulationrun.run_id = {}
        """.format(BistroDB.binary_ids(simulation_id)))
        return str(scenario[0][0])

    def get_simul_by_name(self, name):
        data = self.query("""
            SELECT BIN_TO_UUID(simulationrun.run_id), simulationtag.tag
            FROM simulationrun
            LEFT JOIN simulationtag ON simulationtag.name = simulationrun.name
            WHERE simulationrun.name = '{0}'
        """.format(name))
        return pd.DataFrame(
            data, columns = ["simulation_id", "tag"]
        )
    
    def get_simul_by_scenario(self, scenario):
        data = self.query("""
            SELECT BIN_TO_UUID(simulationrun.run_id), simulationrun.name, simulationrun.datetime, simulationtag.tag
            FROM simulationrun
            LEFT JOIN simulationtag ON simulationtag.name = simulationrun.name
            WHERE simulationrun.scenario = '{0}'
        """.format(scenario))
        return pd.DataFrame(
            data, columns = ["simulation_id", "name", "datetime", "tag"])

    def get_simul_by_id(self, simulation_ids):
        df_cols = ["simulation_id", "datetime", "scenario", "name", "tag"]
        data = self.query("""
            SELECT BIN_TO_UUID(simulationrun.run_id),simulationrun.datetime,simulationrun.scenario,simulationrun.name, simulationtag.tag
            FROM simulationrun
            LEFT JOIN simulationtag ON simulationtag.name = simulationrun.name
            WHERE simulationrun.run_id = UUID_TO_BIN('{}')
        """.format(simulation_ids[0]))
        return pd.DataFrame(
            data, columns = df_cols)

    def load_simulation_df(self):
        data = self.query("""
            SELECT BIN_TO_UUID(simulationrun.run_id), simulationrun.datetime,simulationrun.scenario, simulationrun.name, simulationtag.tag
            FROM simulationrun
            LEFT JOIN simulationtag ON simulationtag.name = simulationrun.name
            """) # WHERE simulationrun.scenario = 'sioux_faux-15k'
        return pd.DataFrame(
            data, columns=['simulation_id','datetime','scenario', 'name', 'tag'])

    def load_links(self, scenario):
        data = self.query("""
            SELECT l.link_id, l.original_node_id, l.destination_node_id, l.freespeed, l.length,
                   fnode.x, fnode.y, tnode.x, tnode.y
            FROM link l
            INNER JOIN node fnode ON fnode.node_id = l.original_node_id
                                  AND fnode.scenario = '{0}'
            INNER JOIN node tnode ON tnode.node_id = l.destination_node_id
                                  AND tnode.scenario = '{0}'
            WHERE l.scenario = '{0}'
            """.format(scenario))
        return pd.DataFrame(
            data,
            columns=['LinkId', 'fromLocationID', 'toLocationID', 'freeSpeed', 'length',
                     'fromLocationX', 'fromLocationY', 'toLocationX',
                     'toLocationY']
            )

    def load_frequency(self, simulation_id):
        db_cols = ['agency_id', 'route_id', 'service_start',
                   'service_end', 'frequency', 'vehicle_type']

        data = self.get_table(
            'fleetmix', cols=db_cols,
            condition="WHERE run_id = UUID_TO_BIN('{}')".format(simulation_id))

        df = pd.DataFrame(
            data, 
            columns=['agency_id', 'route_id', 'start_time',
                     'end_time', 'headway_secs', 'vehicle_type']
            )
        df['exact_times'] = 1

        return df[['route_id', 'start_time', 'end_time', 'headway_secs',
                   'exact_times']]

    def load_fares(self, simulation_id):
        db_cols = ['route_id','age_min','age_max','amount']

        data = self.get_table(
            'transitfare', cols=db_cols,
            condition="WHERE run_id = UUID_TO_BIN('{}')".format(simulation_id))

        df = pd.DataFrame(
            data,
            columns=['routeId', 'age_min','age_max','amount'])
        df['agencyId'] = 217
        df['age'] = '[' + df['age_min'].apply(str) + ':' + df['age_max'].apply(str) + ']'

        return df[['agencyId','routeId','age','amount']]

    def load_incentives(self, simulation_id):
        db_cols = ['trip_mode','age_min','age_max',
                   'income_min','income_max','amount']

        data = self.get_table(
            'incentive', cols=db_cols,
            condition="WHERE run_id = UUID_TO_BIN('{}')".format(simulation_id))

        df = pd.DataFrame(
                data,
                columns=['mode','age_min','age_max','income_min',
                         'income_max','amount'])
        df['age'] = ('[' + df['age_min'].apply(str) + ':' +
                     df['age_max'].apply(str) + ']')
        df['income'] = ('[' + df['income_min'].apply(str) + ':' +
                        df['income_max'].apply(str) + ']')

        return df[['mode','age','income','amount']]

    def load_fleet(self, simulation_id):
        db_cols = ['BIN_TO_UUID(run_id)','agency_id','route_id','service_start',
                   'service_end','frequency','vehicle_type']
        data = self.get_table(
            'fleetmix', cols=db_cols,
            condition="WHERE run_id = UUID_TO_BIN('{}')".format(simulation_id))

        df = pd.DataFrame(
            data, 
            columns=['run_id', 'agencyId', 'routeId', 'start_time',
                     'end_time', 'headway_secs', 'vehicleTypeId']
            )

        return df[['agencyId','routeId','vehicleTypeId']]

    def load_toll_circle(self, simulation_id):
        db_cols = ['type', 'toll', 'center_lat', 'center_lon', 'border_lat',
                   'border_lon']
        data = self.get_table(
            'tollcircle', cols=db_cols,
            condition="WHERE run_id = UUID_TO_BIN('{}')".format(simulation_id))

        df = pd.DataFrame(
            data,
            columns=['type', 'toll', 'center_lat', 'center_lon', 'border_lat',
                   'border_lon']
            )
        return df

    def load_scores(self, simulation_ids):
        db_cols = ['component','weight','z_mean','z_stddev', 'raw_score',
                   'submission_score']
        data = self.get_table(
            'score', cols=db_cols,
            condition="WHERE run_id = UUID_TO_BIN('{}')".format(
                simulation_ids[0])
        )

        df = pd.DataFrame(
            data,
            columns=['Component Name', 'Weight', 'Z-Mean', 'Z-StdDev',
                     'Raw Score', 'Weighted Score']
            )
        return df

    def load_activities(self, scenario):
        db_cols = ['person_id', 'activity_num', 'activity_type', 'link_id', 'desired_start', 'desired_end']

        data = self.get_table(
            'activity', cols=db_cols,
            condition="WHERE scenario = '{}'".format(scenario))

        df = pd.DataFrame(
            data,
            columns=['PID', 'ActNum', 'Type', "LinkId", 'Start_time', 'End_time']
            )
        return df

    def load_facilities(self, scenario):
        db_cols = ['facility_id', 'node_id', 'type']

        data = self.get_table(
            'facility', cols=db_cols,
            condition="WHERE scenario = '{}'".format(scenario))

        df = pd.DataFrame(
            data,
            columns=['FID', 'NodeID', 'Type']
            )
        return df

    def load_household(self, scenario):
        pass
    
    def load_nodes (self, scenario):
        db_cols = ['node_id', 'x', 'y']
        df_columns = ['NodeId', 'y', 'x']
        data = self.get_table(
            'node', cols=db_cols,
            condition="WHERE scenario = '{}'".format(scenario))
        
        df = pd.DataFrame (data, columns=df_columns)
        return df
    def load_leg_links (self, simulation_ids):
        df_columns = ["Leg_num", "LinkId"]
        db_cols = ["leg_num", "link_id"]
        data = self.get_table(
            'leg_link', cols=db_cols,
            condition="WHERE run_id = UUID_TO_BIN('{}')".format(simulation_ids[0])
        )
        df = pd.DataFrame(data, columns=df_columns)
        return df
    def load_legs(self, simulation_ids, links=False):
        db_cols = ['person_id','trip_num', 'leg_num', 'distance','leg_mode',
                   'vehicle', 'leg_end', 'leg_start','fare','fuel_cost','toll']
        df_columns = ['PID','Trip_ID', 'Leg_ID','Distance_m','Mode','Veh',
                      'End_time', 'Start_time','Fare','fuelCost','Toll']
        if links:
            # because we are joining two different tables,
            # it's better to append table name before columns.
            leg_cols = ['leg.'+col for col in db_cols]
            data = self.query(
                """
                SELECT {}, leg_link.link_id
                FROM leg
                LEFT JOIN leg_link ON leg_link.run_id = leg.run_id
                                    AND leg_link.person_id = leg.person_id
                                    AND leg_link.trip_num = leg.trip_num
                                    AND leg_link.leg_num = leg.leg_num
                WHERE leg.run_id = UUID_TO_BIN('{}')
                """.format(', '.join(leg_cols), simulation_ids[0])
            )
            df = pd.DataFrame(data, columns=df_columns+['LinkId'])
        else:
            data = self.get_table(
                'leg', cols=db_cols,
                condition="WHERE run_id = UUID_TO_BIN('{}')".format(
                    simulation_ids[0]))
            df = pd.DataFrame(data, columns=df_columns)

        if links:
            df = df.groupby(df_columns).agg({'LinkId':lambda x: list(x)})
            df.reset_index(inplace=True)

        return df

    def load_vehicles(self, scenario):
        db_cols = ['vehicle_id', 'type']
        data = self.get_table(
            'vehicle', cols=db_cols,
            condition="WHERE scenario = '{}'".format(scenario))
        df = pd.DataFrame(data, columns=['vehicle','vehicleType'])

        return df

    def load_vehicle_types(self, scenario):
        db_cols = ['vehicle_type', 'seating_capacity', 'standing_capacity']
        data = self.get_table(
            'vehicletype', cols=db_cols,
            condition="WHERE scenario = '{}'".format(scenario))
        df = pd.DataFrame(
            data,
            columns=['vehicleTypeId','seatingCapacity','standingRoomCapacity']
        )

        return df

    def load_paths(self, simulation_ids, scenario):
        # mode length vehicle "numPassengers", "vehicleType", "departureTime", "arrivalTime" fuelCost

        db_cols = ['vehicle_id','distance','mode','start_x','start_y','end_x','end_y','start_time','end_time',
                   'num_passengers','driver_id', 'fuel_cost','fuel_consumed', 'path_num']
        df_columns = ['vehicle','length','mode','startX','startY','endX','endY','departureTime','arrivalTime',
                     'numPassengers','driverID','fuelCost','fuelConsumed', 'pathNum']
        
        # For some reason, SQL is refusing to join 3 tables together (Trust me, I tried). 
        # I'll have to do it manaully (which should be more efficient anyways, since I can optimize this)
        leg_cols = ['pathtraversal.'+col for col in db_cols]
        data = self.query(
            """
            SELECT {}, leg_pathtraversal.person_id
            FROM pathtraversal
            LEFT JOIN leg_pathtraversal ON leg_pathtraversal.run_id = pathtraversal.run_id
                                        AND leg_pathtraversal.path_num = pathtraversal.path_num
                                        AND leg_pathtraversal.vehicle_id = pathtraversal.vehicle_id
            WHERE pathtraversal.run_id = UUID_TO_BIN('{}')
            """.format(', '.join(leg_cols), simulation_ids[0])
        )
        path = self.query(
            """
            SELECT pathtraversal_link.vehicle_id, pathtraversal_link.path_num, pathtraversal_link.link_id
            FROM pathtraversal_link
            WHERE pathtraversal_link.run_id = UUID_TO_BIN('{}')
            """.format(simulation_ids[0])
        )
        
        # First, we can cache all the values in pathtraversal_link (will take O(N) instead of O(NM) )
        savedVals = {} # {vehicle_id: {path_num:[links]}}
        for line in path:
            vehicle = str(line[0])
            path_num = str(line[1])
            link = str(line[2])
            if not (vehicle in savedVals.keys()):
                savedVals[vehicle] = {}
            if not (path_num in savedVals[vehicle].keys()):
                savedVals[vehicle][path_num] = []
            savedVals[vehicle][path_num].append(link)

        df = pd.DataFrame(data, columns=df_columns+['passengers'])
        df = df.groupby(df_columns).agg({'passengers':lambda x: list(x)})
        df.reset_index(inplace=True)

        # We won't create path column just yet...
        # First, we'll order the values we need
        toMerge = []
        for index in range(len(df["vehicle"])):
            vehicle = str(df["vehicle"][index])
            path_num = str(df["pathNum"][index])
            if vehicle in savedVals.keys() and path_num in savedVals[vehicle].keys():
                toMerge.append(savedVals[vehicle][path_num])
            else:
                toMerge.append(None)

        # Unfortunately, pandas makes it really hard to change individual values, so we'll just add them in bulk
        df.loc[:, "path"] = toMerge[:]
        
        # Shouldn't be necessary, but we can clear savedVals for that sweet, sweet memory
        del savedVals
        del toMerge

        return df

    def load_person(self, scenario):
        db_cols = ['person_id','age','income']
        data = self.get_table(
            'person', cols=db_cols,
            condition="WHERE scenario = '{}'".format(scenario))

        df = pd.DataFrame(data, columns=['PID','Age','income'])
        return df

    def load_trips(self, simulation_ids):
        # pid realizedmode distance trip_num Duration_sec(end - begin) fuel_cost fare incentive
        db_cols = ['person_id', 'realized_mode', 'distance', 'trip_num',
                   'trip_start', 'trip_end', 'fuel_cost', 'fare', 'toll',
                   'incentives', 'orig_act', 'dest_act']

        data = self.get_table(
            'trip', cols=db_cols,
            condition="WHERE run_id = UUID_TO_BIN('{}')".format(
                simulation_ids[0]))

        df = pd.DataFrame(
            data,
            columns=['PID', 'realizedTripMode', 'Distance_m', 'Trip_ID',
                     'Start_time', 'End_time', 'fuelCost', 'Fare', 'Toll',
                     'Incentive', 'OriginAct', 'DestinationAct'])
        df['Duration_sec'] = df['End_time'] - df['Start_time']

        return df


    def load_mode_choice(self, simulation_ids, realized=False):
        table = 'realizedmodechoice' if realized else 'modechoice'


        db_cols = ['iterations', 'mode', 'count']        
        data = self.get_table(
            table, cols=db_cols,
            condition="WHERE run_id = UUID_TO_BIN('{}')".format(
                simulation_ids[0]))

        df = pd.DataFrame(data, columns=['iterations', 'mode', 'count'])
        df = df.pivot_table(index='iterations', columns='mode', values='count') 
        del df.columns.name
        return df.reset_index()

    def load_hourly_mode_choice(self, simulation_ids):
        db_cols = ['mode','hour','count']
        data = self.get_table(
            'hourlymodechoice', cols=db_cols,
            condition="WHERE run_id = UUID_TO_BIN('{}')".format(
                simulation_ids[0]))

        df = pd.DataFrame(
            data, columns=['Modes', 'Hour', 'Count'])
        df = df.pivot_table(index='Modes', columns='Hour', values='Count')
        del df.columns.name
        df.rename(
            columns={hour:'Bin_'+str(hour) for hour in df.columns},
            inplace=True)

        return df.T

    def load_travel_times(self, simulation_ids):
        db_cols = ['mode','hour','averagetime']
        data = self.get_table(
            'traveltime', cols=db_cols,
            condition="WHERE run_id = UUID_TO_BIN('{}')".format(
                simulation_ids[0]))

        df = pd.DataFrame(data, columns=['TravelTimeMode\\Hour','Hour','Traveltime'])
        df = df.pivot_table(index='TravelTimeMode\\Hour', columns='Hour', values='Traveltime')
        del df.columns.name
        return df.reset_index()

    def load_vehicle_cost(self, scenario):
        db_cols = ['vehicle_type', 'operation_cost']
        data = self.get_table(
            'vehiclecost', cols=db_cols,
            condition="WHERE scenario = '{}'".format(scenario))

        df = pd.DataFrame(data, columns=['vehicleTypeId','opAndMaintCost'])
        return df        

    def load_trip_to_route(self, scenario):
        db_cols = ['trip_id','route_id']
        data = self.get_table(
            'transittrip', cols=db_cols,
            condition="WHERE scenario = '{}'".format(scenario))

        df = pd.DataFrame(data, columns=['trip_id','route_id'])
        return df

    def load_agency(self, scenario):
        db_cols = ['agency_id']

        return [row[0] for row in self.get_table(
            'agency', cols=db_cols,
            condition="WHERE scenario = '{}'".format(scenario))]

    def load_route_ids(self, scenario):
        db_cols = ['route_id']

        return [row[0] for row in self.get_table(
            'transitroute', cols=db_cols,
            condition="WHERE scenario = '{}'".format(scenario))]
