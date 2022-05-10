#!/usr/bin/env python3
import sys
import re
import json
import pandas as pd
import numpy as np
from datetime import date
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from pytz import timezone
import datetime

topic = "breadcrumb"
DBname = "postgres"
DBuser = "postgres"
DBpwd = "PLEASE INPUT PASSWORD"
TableBreadCrumbName = 'BreadCrumb'
TableTripName = "Trip"
Datafile = "filedoesnotexist"
CreateDB = False

# todays date
pst_tz = timezone('US/Pacific')
today = datetime.datetime.now(pst_tz)
today = today.strftime('%Y-%m-%d')



def create_Kafka_consumer_with(config):
    consumer = Consumer(config)
    return consumer


def consume_messages_with(consumer, topic = "breadcrumb"):
    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)
    
    # Subscribe to topic
    consumer.subscribe([topic], on_assign=reset_offset)
    
    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                print("consuming")
                #print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    #topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))        
                # WRITE DATA TO A FILE
                msg_bytes = msg.value()
                msg_string = msg.value().decode('utf-8')
                # LINE TO SUBSTITUTE DOUBLE QUOTES FOR SINGLE QUOTES FROM:
                # https://stackoverflow.com/questions/39491420/python-jsonexpecting-property-name-enclosed-in-double-quotes
                valid_json = re.sub( "(?<={)\'|\'(?=})|(?<=\[)\'|\'(?=\])|\'(?=:)|(?<=: )\'|\'(?=,)|(?<=, )\'", "\"", msg_string)
                json_data = valid_json

                json_obj = json.loads(json_data)
                json_formatted = json.dumps(json_obj, indent=2)

                fil = open('{}.txt'.format(today), "a")
                fil.write(json_formatted)
                fil.write('\n')
                fil.close()
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

# load data into a dataframe
def load_data():
    Datafile = '{}.txt'.format(today)
    return pd.read_json(Datafile)
# drop all the unnecessary columns 
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

# Adds 2 new columns (Speed and Date)
def transformations(df):
    speed = []
    spee = pd.to_numeric(df['VELOCITY'])
    for index, row in df.iterrows(): 
        speed.append(spee[index] * 2.237)
    df['SPEED'] = speed
    df = fix_date(df)
    return df

# Create a new column (Date) and appends to it if ACT_TIME overflows (> 86399)
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

#
def get_df_add_to_db():
    df = load_data()
    df = drop_columns(df)
    before_transformation_validation(df)
    df = transformations(df)

    # Connect to the database
    conn = dbconnect()

    # Create breadcrumb and trip table
    createTable(conn)
    createTripTable(conn)

    # Load the data from dataframe to the database 
    load_db(conn, df)

    # prints the df
    #print(df)




if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()
    
    # Parse the configuration.
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])
    
    consumer = create_Kafka_consumer_with(config) 
    consume_messages_with(consumer)

    # Receives the data and store it as a dataframe
    # Transform & validate it and store it into the database
    # Project part 2 
    # get_df_add_to_db()


