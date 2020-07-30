#!/usr/bin/env python3

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from circus.watcher import Watcher
import logging
import pika
import subprocess
logger = logging.getLogger()
format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=format)
logging.getLogger('pika').setLevel(logging.INFO)

watcher = Watcher('cheeta', '/home/artemis/development/arecibo/dev/build/linux/gcc/release/work/arecibo/alfaburst_16bit --config /home/artemis/configs/beam0.xml -p empty -s udp')
#watcher to run the data recorder, keep as a global variable

def record_fil(status):
    """
    Starts and stops the Watcher that runs the cheeta filterbank recorder

    Parameters:
    status (string): a 'True'/'False' string the determines if data should be taken
    """   
    if status == 'True':
        logging.info('Recieved True, starting watcher')
        try:
            watcher.reload() #use reload incase the watcher had been previously stopped
        except Exception as e:
            logging.info(f'Watcher cannot be started: {e}')
    else:
        logging.info('Recieved False, stopping watcher')
        try:
            watcher.kill_processes()
        except Exception as e:
            logging.info(f'Arbiter cannot be stopped: {e}')

def stage_initer():
    """
    Communicates with AMQP on the head node to recieve start ('True') and stop ('False') signals to start and stop recording data
    """
    credentials = pika.PlainCredentials('artemis','artemis')
    connection = pika.BlockingConnection(pika.ConnectionParameters('10.1.1.1', 5672, '/', credentials))
    channel = connection.channel()
    
    channel.queue_declare(queue='recorder_beam0', durable=True)
    
    def callback(ch, method, properties, body):
        """
        Handles changes of state 
        """
        status = body.decode()
        logging.info(f'got it {status}')
        ch.basic_ack(delivery_tag = method.delivery_tag)
        record_fil(status)
        logging.info("Ack'ed")
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(on_message_callback=callback,
                          queue='recorder_beam0')
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
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Be verbose')
    parser.add_argument('-d', '--daemon', dest='daemon', action='store_false', help='Run with AMQP')
    parser.set_defaults(verbose=False)
    parser.set_defaults(daemon=True)
    values = parser.parse_args()
    
    if values.daemon:
        logging.info('Running in daemon mode')
        stage_initer()
    else:
        begin_main(values)

