#!/usr/bin/env python3

import redis
import yaml
import time
import logging
import pandas as pd
from influxdb import InfluxDBClient, DataFrameClient
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from astropy.time import Time, TimeMJD

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

r = redis.StrictRedis(host='serendip6',port='6379')#6379 is the default port

def get_pipe():
    pipe = r.pipeline()
    
    pipe.hmget('SCRAM:IF2', 'IF2SIGSR') #sigSrcGregorian
    pipe.hmget('SCRAM:IF2', 'IF2ALFON') #recALFAEnabled
    pipe.hmget('SCRAM:IF1', 'IF1RFFRQ') #rfCenterFreq
    pipe.hmget('SCRAM:PNT', 'PNTMJD') #MJD
    
    for j in range(0,7):
        pipe.hmget('SCRAM:DERIVED', f'RA{j}') #RAs for all beams
        pipe.hmget('SCRAM:DERIVED', f'DEC{j}') #Decs for all beams
        
    return pipe

with open("/home/artemis/programs/greenburst/dev_trunk/config/conf.yaml", 'r') as stream:
    data_loaded = yaml.load(stream)
    login_detail = data_loaded['influxdb']
    client = DataFrameClient(host=login_detail['host'],
                             port=login_detail['port'],
                             username=login_detail['uname'],
                             password=login_detail['passw'],
                             database=login_detail['db'])

def main():
    pipe = get_pipe()
    value = pipe.execute()
    keys = ['sig_source_gregorian', 'rec_AFLA_enabled', 'rf_center_freq', 'MJD']
    for j in range(0,7):
        keys.append(f'ra{j}')
        keys.append(f'dec{j}')
    telescope_status = {}
    for j, k in enumerate(keys):
        telescope_status[k] = float(value[j][0].decode())
     
    if telescope_status['sig_source_gregorian'] == 1.0 and telescope_status['rec_ALFA_enabled'] == 1.0:
        telescope_status['data_valid'] = 1.0
    else:
         telescope_status['data_valid'] = 0.0
             
    df=pd.DataFrame(telescope_status,index=[pd.to_datetime(Time(telescope_status['MJD'],format='mjd').iso)])
    logging.debug(df.values.tolist())
    val=client.write_points(df,measurement='telescope',time_precision='n')

if __name__ == '__main__':
    main()
    scheduler = BackgroundScheduler()
    sleep_time=int(5*3600+1)
    scheduler.add_job(main, 'interval', seconds=1,start_date=datetime.now(),end_date=datetime.now()+timedelta(seconds=sleep_time))
    scheduler.start()

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(sleep_time) # do this for 5 min then break
            break
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()
