#!/usr/bin/env python3

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from circus.watcher import Watcher
import logging
import pika
import subprocess
import yaml

logger = logging.getLogger()
format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=format)
logging.getLogger('pika').setLevel(logging.INFO)

with open("/home/artemis/programs/alfaburst/scripts/config/conf.yaml", 'r') as stream:
    data_loaded = yaml.load(stream)
    login_detail = data_loaded['rabbit']
    headnode = login_detail['headnode']
    user = login_detail['user']
    password = login_detail['password']


def create_watcher(beam):
    """
    creates a global watcher object for a specified beam

    Parameters:
    beam (int): the beam number to make a watcher 
    """
    global watcher
    watcher = Watcher('cheeta', f'/home/artemis/development/arecibo/dev/build/linux/gcc/release/work/arecibo/alfaburst_16bit --config /home/artemis/configs/beam{beam}.xml -p empty -s udp')
    #watcher to run the data recorder, keep as a global variable

def record_fil(status, beam):
    """
    Starts and stops the Watcher that runs the cheeta filterbank recorder

    Parameters:
    status (string): a 'True'/'False' string the determines if data should be taken
    """   
    if status == 'True':
        print(watcher.status)
        logging.info('Recieved True, starting watcher')
        try:
            print('Starting watcher')
            watcher.start()
        except Exception as e:
            logging.info(f'Watcher cannot be started: {e}')
    else:
        logging.info('Recieved False, stopping watcher')
        try:
            watcher.stop()
            create_watcher(beam) #create a new watcher b/c we can't revive the old one
        except Exception as e:
            logging.info(f'Arbiter cannot be stopped: {e}')

def stage_initer(beam):
    """
    Communicates with AMQP on the head node to recieve start ('True') and stop ('False') signals to start and stop recording data
    """
    credentials = pika.PlainCredentials(user,password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(headnode, 5672, '/', credentials))
    channel = connection.channel()
    
    channel.queue_declare(queue=f'recorder_beam{beam}', durable=True)
    
    def callback(ch, method, properties, body):
        """
        Handles changes of state 
        """
        status = body.decode()
        logging.info(f'got it {status}')
        ch.basic_ack(delivery_tag = method.delivery_tag)
        watcher = record_fil(status, beam)
        logging.info("Ack'ed")
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(on_message_callback=callback,
                          queue=f'recorder_beam{beam}')
    try:    
        channel.start_consuming()
    except (StreamLostError, ConnectionResetError) as e:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.1.1.1'))
        channel = connection.channel()

def begin_main(values):
    """
    Sets the logging levels

    Paramters:
    values (argparse object): user defined values   
    """
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    if values.verbose:
        logging.basicConfig(level=logging.DEBUG, format=format)
    else:
        logging.basicConfig(level=logging.INFO, format=format)

if __name__ == "__main__":
    parser=ArgumentParser(description='Record data with the Cheeta Recorder', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-b', '--beam', dest='beam', type=int, help='sets the beam number, default: 0')
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Be verbose')
    parser.add_argument('-d', '--daemon', dest='daemon', action='store_false', help='Run with AMQP')
    parser.set_defaults(beam=0)
    parser.set_defaults(verbose=False)
    parser.set_defaults(daemon=True)
    values = parser.parse_args()

    create_watcher(values.beam)

    if values.daemon:
        logging.info('Running in daemon mode')
        stage_initer(values.beam)
    else:
        begin_main(values)

