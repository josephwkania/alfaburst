#!/usr/bin/env python3

import redis
import yaml
import time
import logging
import pika
import queue
import pandas as pd
from influxdb import InfluxDBClient, DataFrameClient
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from astropy.time import Time, TimeMJD

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

r = redis.StrictRedis(host='serendip6',port='6379')#6379 is the default port

valid_queue = queue.Queue(15)#make sure ALFA is valid for 15 sec before start recording
center_freq = 0.0 #check if center freq moves 

def get_pipe():
    """
    Creates the pipe object and get the desired values from redis

    Returns:
    pipe (object): redis telescope values
    """
    pipe = r.pipeline()
    
    pipe.hmget('SCRAM:IF2', 'IF2SIGSR') #sigSrcGregorian
    pipe.hmget('SCRAM:IF2', 'IF2ALFON') #recALFAEnabled
    pipe.hmget('SCRAM:IF1', 'IF1RFFRQ') #rfCenterFreq
    pipe.hmget('SCRAM:PNT', 'PNTMJD') #MJD
    
    for j in range(0,7):
        pipe.hmget('SCRAM:DERIVED', f'RA{j}') #RAs for all beams
        pipe.hmget('SCRAM:DERIVED', f'DEC{j}') #Decs for all beams
        
    return pipe

with open("/home/artemis/programs/alfaburst/scripts/config/conf.yaml", 'r') as stream:
    """
    Get influx and rabbitmq login details
    """
    data_loaded = yaml.load(stream)
    login_detail = data_loaded['influxdb']
    client = DataFrameClient(host=login_detail['host'],
                             port=login_detail['port'],
                             username=login_detail['uname'],
                             password=login_detail['passw'],
                             database=login_detail['db'])
    login_detail = data_loaded['rabbit']
    headnode = login_detail['headnode']
    user = login_detail['user']
    password = login_detail['password']

def create_channels():
    """
    Creates the channel object, 

    Returns:
    channel (object): channel to communicate with all the beams 
    """
    
    credentials = pika.PlainCredentials(user,password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(headnode, 5672, '/', credentials))
    
    channel = connection.channel()
    for j in range(0,7):#chreate channels for each beam
        channel.queue_declare(queue=f'recorder_beam{j}', durable=True)
    return channel


def record_data(status, channel):
    """
    Sends start/stop recording signal to all the beams
    """                                                                                                                                                               
    for j in range(0,7): 
        channel.basic_publish(exchange='',
	                      routing_key=f'recorder_beam{j}',
	    	       	      body=f'{status}')


def main(channel):
    """
    Queries redis database and sends data to influxDB, activates filterbank recorders if ALFA is active

    Paramters:
	channel: rabbitmq channel object to send siganl to start/stop filterbank recorders
    """
    pipe = get_pipe()
    value = pipe.execute()
    keys = ['sig_source_gregorian', 'rec_ALFA_enabled', 'rf_center_freq', 'MJD']
    for j in range(0,7):
        keys.append(f'ra{j}')
        keys.append(f'dec{j}')
    telescope_status = {}
    for j, k in enumerate(keys):
        telescope_status[k] = float(value[j][0].decode())
     
    if telescope_status['sig_source_gregorian'] == 0.0 and telescope_status['rec_ALFA_enabled'] == 1.0 and telescope_status['rf_center_freq'] == center_freq:
        telescope_status['data_valid'] = 1.0
        if not valid_queue.full():
            valid_queue.put_nowait(1.0)
    else:
        telescope_status['data_valid'] = 0.0
        with valid_queue.mutex: #a threadsafe way to clear the queue
            valid_queue.queue.clear()

    center_freq = telescope_status['rf_center_freq'] 

    df=pd.DataFrame(telescope_status,index=[pd.to_datetime(Time(telescope_status['MJD'],format='mjd').iso)])
    logging.debug(df.values.tolist())
    val=client.write_points(df,measurement='telescope',time_precision='n')
    if valid_queue.full():
        record_data(True, channel)
    else:
        record_data(False, channel)

if __name__ == '__main__':
    channel = create_channels()
    main(channel)
    job_defaults = {
	        'max_instances': 3
		    }
    scheduler = BackgroundScheduler(job_defaults=job_defaults)
    sleep_time=int(5*3600+1)
    scheduler.add_job(main, 'interval', seconds=1, args=[channel], start_date=datetime.now(),end_date=datetime.now()+timedelta(seconds=sleep_time))
    scheduler.start()

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(sleep_time) # do this for 5 min then break
            break
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()
