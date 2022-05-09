import pandas as pd
import re
import numpy as np
import json
import datetime
import psycopg2
from datetime import timedelta, time, date
from pytz import timezone


DBname = "postgres"
DBuser = "postgres"
DBpwd = "501Cspsu0507"
TableBreadCrumbName = 'BreadCrumb'
TableTripName = "Trip"
Datafile = "filedoesnotexist"
CreateDB = False
# todays date
pst_tz = timezone('US/Pacific')
today = datetime.datetime.now(pst_tz)
today = today.strftime('%Y-%m-%d')


def load_data():
    Datafile = '{}.txt'.format(today)
    return pd.read_json(Datafile)
def drop_columns(df):
    return df.drop(columns=['RADIO_QUALITY','GPS_SATELLITES', 'GPS_HDOP', 'SCHEDULE_DEVIATION', 'METERS']) 
def before_transformation_validation(df):
    # converting the df to numeric or string
    direction = pd.to_numeric(df['DIRECTION'])
    event_no_trip = pd.to_numeric(df['EVENT_NO_TRIP'])
    event_no_stop = pd.to_numeric(df['EVENT_NO_STOP'])
    velocity = pd.to_numeric(df['VELOCITY'])
    act_time = pd.to_numeric(df['ACT_TIME'])
    gps_latitude = pd.to_numeric(df['GPS_LATITUDE'])
    gps_longitude = pd.to_numeric(df['GPS_LONGITUDE'])
    vehicle_id = pd.to_numeric(df['VEHICLE_ID'])
    opd_date = df['OPD_DATE']
    #gps_satellites = pd.to_numeric(df['GPS_SATELLITES'])
    # flag 
    # I have this variable because we might want to avoid adding data to database if any assertions is false. Thus returning flag as false
    # currently it doesn't to anything
    flag = True
    for index, row in df.iterrows(): 
        # Direction will always be between 0 to 359
        if(pd.isna(direction[index])):
            df.at[index, 'DIRECTION'] = -1
        if(pd.isna(velocity[index])):
            df.at[index, 'VELOCITY'] = -1
        #if(gps_satellites[index] < 2 or gps_satellites[index] > 13):
            #print("ASSERTION FAILED: GPS_SATELLITES")
            #print(gps_satellites[index])
        if(direction[index] < 0 or direction[index] > 359):
            print("DIRECTION IS FALSE")
            print(direction[index])
        if(velocity[index] < 0 or velocity[index] > 37):
            print("ASSERTION FAILED: VELOCITY")
            print(velocity[index])
        if(gps_latitude[index] <45 or gps_latitude[index] >= 46):
            print("ASSERTION FAILED: GPS_LATITUDE")
            print(gps_latitude[index])
        if(pd.isna(event_no_trip[index])):
            print("ASSERTION FAILED: EVENT_NO_TRIP")
            print(event_no_trip[index])
        if(pd.isna(vehicle_id[index])):
            print("ASSERTION FAILED: VEHICLE_ID")
            print(vehicle_id[index])
        if(pd.isna(gps_latitude[index]) or pd.isna(gps_longitude[index])):
            if(pd.isna(gps_latitude[index]) and not pd.isna(gps_longitude[index])):
                print("ASSERTION FAILED: gps_latitude is empty")
                print(index)
            elif(not pd.isna(gps_latitude[index]) and pd.isna(gps_longitude[index])):
                print("ASSERTION FAILED: gps_longitude is empty")
                print(index)
            else:
                df.at[index, "GPS_LONGITUDE"] = -1
                df.at[index, "GPS_LATITUDE"] = -1
        if(pd.isna(event_no_trip[index]) or pd.isna(event_no_stop[index])):
            if(pd.isna(event_no_trip[index]) and not pd.isna(event_no_stop[index])):
                print("ASSERTION FAILED: event_no_trip is empty")
                print(index)
            elif(not pd.isna(event_no_trip[index]) and pd.isna(event_no_stop[index])):
                print("ASSERTION FAILED: event_no_stop is empty")
                print(index)
        if(opd_date[index] == ""):
            print("ASSERTION FAILED: opd_date")
            print(index)
    return flag
def transformations(df):
    speed = []
    spee = pd.to_numeric(df['VELOCITY'])
    for index, row in df.iterrows(): 
        speed.append(spee[index] * 2.237)
    df['SPEED'] = speed
    df = fix_date(df)
    return df

def dbconnect():
    connection = psycopg2.connect(
            host="localhost",
            database=DBname,
            user=DBuser,
            password=DBpwd,
    )
    connection.autocommit = True
    return connection

def createTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
                DROP TABLE IF EXISTS {TableBreadCrumbName};
                CREATE TABLE {TableBreadCrumbName} 
                (tstamp TIMESTAMP, 
                latitude DECIMAL, 
                longitude DECIMAL, 
                direction INTEGER, 
                speed DECIMAL, 
                trip_id INTEGER);""")
    print(f"Create {TableBreadCrumbName} Table")

def createTripTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(f""" 
                DROP TABLE IF EXISTS {TableTripName};
                CREATE TABLE {TableTripName}
                (trip_id INTEGER,
                route_id INTEGER,
                vehicle_id INTEGER,
                service_id INTEGER,
                direction INTEGER);""")
    print(f"Create {TableTripName} Table")

def load_db(conn, df):
    with conn.cursor() as cursor:
        print(f"Loading {len(df)} rows")
        for index, row in df.iterrows():
            cursor.execute(f"""
            INSERT INTO {TableBreadCrumbName}(
            tstamp, 
            latitude, 
            longitude, 
            direction, 
            speed, 
            trip_id) 
            VALUES ('{df['TIME_STAMP'][index]}', {df['GPS_LATITUDE'][index]}, {df['GPS_LONGITUDE'][index]}, {df['DIRECTION'][index]}, {df['SPEED'][index]}, {df['EVENT_NO_TRIP'][index]});""") 
            cursor.execute(f"""
            INSERT INTO {TableTripName} (
            trip_id,
            route_id,
            vehicle_id,
            service_id,
            direction)
            VALUES ({df['EVENT_NO_TRIP'][index]}, 0, {df['VEHICLE_ID'][index]}, 0, 0);""")



def fix_date(df):
    old_date = df['OPD_DATE']
    old_time = df['ACT_TIME']
    combined_date = [] 
    for index, row in df.iterrows():
        datetime_object = datetime.datetime.strptime(old_date[index], '%d-%b-%y').date()
        # date.app
        new_delta = datetime.timedelta(seconds = int(old_time[index]))
        new_time = datetime.time(hour= new_delta.seconds//3600, minute= (new_delta.seconds//60)%60, second = new_delta.seconds%60)
        carry = new_delta.days
        date_time = datetime.datetime.combine(datetime_object , new_time)
        if (carry == 1):
            date_time += timedelta(days=1)
        combined_date.append(date_time)
    df['TIME_STAMP'] = combined_date
    return df

df = load_data()
df = drop_columns(df)
before_transformation_validation(df)
df = transformations(df)
print(df.head(50))
# Connect to the database
conn = dbconnect()
# Create breadcrumb and trip table
createTable(conn)
createTripTable(conn)

# Load the data from dataframe to the database 
load_db(conn, df)


