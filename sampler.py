#!/usr/bin/env python3

from influxdb import InfluxDBClient
import time
from json import loads
import re
import requests
import threading
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import urllib3
from datetime import datetime
from tzlocal import get_localzone
import pytz
import os
import logging

urllib3.disable_warnings()

API_GET_NODES = 'v1/nodes/list'
API_GET_NODE_WELL_KNOWN = 'v2/node/.well-known'
API_GET_NODE_EVENT_POP = 'v2/node/event/pop'
API_GET_NODE_EVENT_LENGTH = 'v2/node/event/length'
API_GET_NODE_EVENT_CLEAR = 'v2/node/event/clear'

EVENT_QUEUE_EMPTY = "Queue Empty"
EVENT_SAMPLER_START = "Sensor Sampler State"
EVENT_SAMPLER_GROVE = "sensor_sampler_grove"
EVENT_SAMPLER_FUNCTION = "sensor_sampler_function"
EVENT_SAMPLER_VALUE = "sensor_sampler_value"
EVENT_SAMPLER_UPTIME = "sensor_sampler_uptime"

EVENT_DATA_KEY = "event_data"

STATE_SAMPLER_START = 0
STATE_SAMPLER_GROVE = 1
STATE_SAMPLER_FUNC  = 2
STATE_SAMPLER_VALUE = 3

MEASUREMENT_NAMES = [
    {
        'groves':['GroveTemp1WireD0','GroveTemp1WireD1'],
        'funcs':['temp'],
        'name':'soil_temperature'
    },
    {
        'groves':['GroveMoistureSeesawI2C0','GroveMoistureSeesawI2C1'],
        'funcs':['temperature'],
        'name':'soil_temperature'
    },
    {
        'groves':['GroveMoistureSeesawI2C0','GroveMoistureSeesawI2C1'],
        'funcs':['moisture'],
        'name':'soil_moisture'
    },
    {
        'groves':['GroveMoistureA0','GroveMoistureA1'],
        'funcs':['moisture'],
        'name':'soil_moisture'
    },
    {
        'groves':['GroveMoistureCapacitiveA0','GroveMoistureCapacitiveA1'],
        'funcs':['moisture'],
        'name':'soil_moisture'
    },
    {
        'groves':['GroveM5UnitENV2I2C0','GroveM5UnitENV2I2C1'],
        'funcs':['temperature_barom', 'temperature_humid'],
        'name':'air_temperature'
    },
]

#Settings
user_token = os.getenv('WIO_USER_TOKEN', '')
node_name_frags = [i for i in os.getenv('WIO_NODE_NAME_FRAGMENTS', '[]').split(",")] 
sample_delay_s = os.getenv('WIO_SAMPLE_DELAY_S', 60)
check_delay_s = os.getenv('WIO_CHECK_DELAY_S', 300)
wio_host = os.getenv('WIO_HOST', '')
wio_proto = os.getenv('WIO_PROTO', 'https')
wio_port = os.getenv('WIO_PORT', '447')
samples_db_file = os.getenv('WIO_SAMPLES_JSON_FILE', '/samples_db.json')

base_url = '{0}://{1}:{2}'.format(
            wio_proto,
            wio_host,
            wio_port,
        )

influx_host = os.getenv('INFLUX_HOST', '')
influx_port = os.getenv('INFLUX_PORT', 8086)
database_name = os.getenv('INFLUX_DB_NAME', 'test')

executor = ThreadPoolExecutor(max_workers=1)
client = InfluxDBClient(host=influx_host, port=influx_port)
client.create_database(database_name)
samples_to_write = []
local_tz = get_localzone()

def send_samples(sample):
    global samples_to_write
    
    if sample != None:
        samples_to_write.append(sample)
    
    if samples_to_write:
        ret = client.write_points(samples_to_write, database=database_name, time_precision='ms', batch_size=20, protocol='json')
        if ret:
            samples_to_write.clear()
    
def sampler_thread(delay, run_event, node_base_url, node_token, node_name, node_sn):
    url_len = '{0}/{1}'.format(node_base_url, API_GET_NODE_EVENT_LENGTH)
    url_pop = '{0}/{1}'.format(node_base_url, API_GET_NODE_EVENT_POP)
    sampler_state = STATE_SAMPLER_START
    logger_thread = logging.getLogger(node_name)
    while True:
#         print("Node Thread: %s" %(node_token))
        grove_name = None
        function_name = None
        value = None
        timestamp = None
        try:
#             raw = requests.get(url_len, params={'access_token':node_token}, timeout=1, verify=False)
#             event_q_len = raw.json()
#             if event_q_len['length'] >= 5:
            if True:
                while True:
                    raw = requests.get(url_pop, params={'access_token':node_token}, timeout=20, verify=False)
                    data_json = raw.json()
                    if 'error' in data_json and data_json['error'] == "Node Unknown":
#                         print("Node Thread: %s - Empty Queue" %(node_token))
                        break
                    
                    if (EVENT_DATA_KEY in data_json) and (EVENT_SAMPLER_START in data_json[EVENT_DATA_KEY]) and ("Start" in data_json[EVENT_DATA_KEY][EVENT_SAMPLER_START]):
#                       time_stamp = datetime.strptime(data_json['timestamp'], "%Y-%m-%dT%H:%M:%S.%f")
                        sampler_state = STATE_SAMPLER_GROVE
                        continue
                    elif sampler_state == STATE_SAMPLER_GROVE:
                        if (EVENT_DATA_KEY in data_json) and (EVENT_SAMPLER_GROVE in data_json[EVENT_DATA_KEY]):
                            grove_name = data_json[EVENT_DATA_KEY][EVENT_SAMPLER_GROVE]
                            sampler_state = STATE_SAMPLER_FUNC
                        elif (EVENT_DATA_KEY in data_json) and (EVENT_SAMPLER_UPTIME in data_json[EVENT_DATA_KEY]):
                            grove_name = "SensorSampler"
                            function_name = "uptime"
                            value = float(data_json[EVENT_DATA_KEY][EVENT_SAMPLER_UPTIME])
                            timestamp = local_tz.localize(datetime.strptime(data_json['timestamp'], "%Y-%m-%dT%H:%M:%S.%f"))
                            data = {
                                    "measurement": "uptime",
                                    "tags": {
                                            "node_name": node_name,
                                            "node_sn": node_sn,
                                            "grove": grove_name,
                                            "function": function_name,
                                    },
                                    "fields": {
                                        "value": value,
                                    },
                                    "time": timestamp.astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                            }
                            executor.submit(send_samples, data)
                            # print("%s - %s : %s : %s" % (timestamp, grove_name, function_name, value))
                            sampler_state = STATE_SAMPLER_GROVE
                            continue
                        else:
                            sampler_state = STATE_SAMPLER_START
                        continue
                    elif sampler_state == STATE_SAMPLER_FUNC:
                        if (EVENT_DATA_KEY in data_json) and (EVENT_SAMPLER_FUNCTION in data_json[EVENT_DATA_KEY]):
                            function_name = data_json[EVENT_DATA_KEY][EVENT_SAMPLER_FUNCTION]
                            sampler_state = STATE_SAMPLER_VALUE
                        else:
                            sampler_state = STATE_SAMPLER_START
                        continue
                    elif sampler_state == STATE_SAMPLER_VALUE:
                        if (EVENT_DATA_KEY in data_json) and (EVENT_SAMPLER_VALUE in data_json[EVENT_DATA_KEY]):
                            value = float(data_json[EVENT_DATA_KEY][EVENT_SAMPLER_VALUE])
                            timestamp = local_tz.localize(datetime.strptime(data_json['timestamp'], "%Y-%m-%dT%H:%M:%S.%f"))
            
                            measurment_name = function_name
                            for entry in MEASUREMENT_NAMES:
                                if grove_name in entry['groves'] and function_name in entry['funcs']:
                                    measurment_name = entry['name']
                            data = {
                                    "measurement": measurment_name,
                                    "tags": {
                                            "node_name": node_name,
                                            "node_sn": node_sn,
                                            "grove": grove_name,
                                            "function": function_name,
                                    },
                                    "fields": {
                                        "value": value,
                                    },
                                    "time": timestamp.astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                            }
                            executor.submit(send_samples, data)
#                             print("%s - %s : %s : %s -- %s" % (timestamp, grove_name, function_name, value, measurment_name))
                            sampler_state = STATE_SAMPLER_GROVE
                        else:
                            sampler_state = STATE_SAMPLER_START
                        continue
        except ValueError:
            pass
        except Exception as e:
            logger_thread.error(e)
            pass
        
        if run_event.wait(delay):
            logger_thread.info("Thread Ending")
            break

def main():
    global samples_to_write
    
    logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M', level=logging.INFO)
    logger_main = logging.getLogger('main')
    
    thread_dict = {}
    run_event = threading.Event()
    run_event.clear()
    
    node_reg = re.compile("\/v1\/node[^ ]*")
    url = '{0}/{1}'.format(base_url, API_GET_NODES)

    logger_main.info(base_url)
    logger_main.info(influx_host)
    logger_main.info(database_name)
    logger_main.info(node_name_frags)
    logger_main.info(url)
    
    if os.path.exists(samples_db_file):
        try:
            with open(samples_db_file,"r") as json_file:
                samples_to_write = json.load(json_file)
        except Exception as e:
            logger_main.error(e)
            logger_main.error("Failed to load samples json file!")
            pass
        
    try:
        
        while 1:
            #Check for new Nodes
            try:
                logger_main.info(local_tz.localize(datetime.now()).strftime("%d/%m/%Y %H:%M:%S"))

                r = requests.get(url, params={'access_token':user_token}, timeout=5, verify=False)
                nodes_json = r.json()
                for node in nodes_json['nodes']:
                    logger_main.info('Found Node: %s, Online: %s' % (node['name'], node['online']))
                    if node_name_frags != None and not any([val in node['name'] for val in node_name_frags]):
                        logger_main.info('Skipping Node: %s, no filter match' % node['name'])
                        continue
                    if node['name'] in thread_dict and (thread_dict[node['name']].is_alive()):
                        logger_main.info('Node: %s, already monitored' % node['name'])
                        continue
                    
                    node_base_url = node['dataxserver'] if node['dataxserver'] != None else base_url
                    x = threading.Thread(target=sampler_thread, args=(sample_delay_s, run_event, node_base_url, node['node_key'], node['name'], node['node_sn']))
                    thread_dict[node['name']] = x
                    x.start()
            except Exception as e:
                logger_main.error(e)
                logger_main.error("Failed to get nodes list!")
                pass
            
            time.sleep(check_delay_s)
#             print("\n\n")
    except KeyboardInterrupt:
        logger_main.info("Shutdown Requested attempting to close threads.")
        run_event.set()
        for key, val in thread_dict.items():
            val.join()
        logger_main.info("Threads successfully closed")
        if samples_to_write:
            fut  = executor.submit(send_samples, None)
            fut.result()
            if samples_to_write:
                try:
                    with open(samples_db_file, 'w') as fp:
                        json.dump(samples_to_write, fp)
                except Exception as e:
                    logger_main.error(e)
                    logger_main.error("Failed to save leftover sample!")
                    pass                

if __name__ == "__main__":
    main()
