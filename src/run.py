import time
from config2.config import config
from src.dbservice.dbservicefactory import DbServiceFactory
import importlib
from resources.active_plugins import plugins
import requests
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed


def generate_signal(signal_name, value, payload):
    headers = {
        'Content-Type': 'application/json',
    }
    json = { "deployment" : { "ACD" : "Predictive", "tenantID" : "avaya" }, 
    "name" : signal_name, "__v" : 0, "level" : value, 
    "payload" : {"state": payload['value'], "data" : [[ payload ]] }, "signaler" : "Pre-Watch", "syncRequest":True, 
    "timestamps" : { "postTime" : round(time.time() * 1000) }
    }
    response = None
    try:
        response = requests.post('http://%s:%i/signal'%(config.oversight.host,config.oversight.port), headers=headers, json=json)
    except Exception as e:
        logging.error("POST request to oversight:http://%s:%i/signal failed for signal %s"%(config.oversight.host,config.oversight.port,signal_name))
    if(response != None and response.ok):
        logging.info("Predictive Signal %s generated."%(signal_name))
        
def run(): 
    #create the db service factory
    dbservicefactory = DbServiceFactory()
    
    # create a list of plugins
    active_plugins = []
    for plugin in plugins:
        try:
            active_plugins.append(importlib.import_module("."+plugin,"resources.plugins").Plugin(dbservicefactory))
        except Exception as exc:
            logging.error('%r failed to load. Exception:' % (plugin))
            traceback.print_exc()
    logging.info("Default plugins loaded")
    logging.info(plugins)

    #import external plugins
    external_plugins = config.externalplugins
    if(external_plugins != None and external_plugins != ""):
        external_plugins = external_plugins.split(',')
        for plugin in external_plugins:
            try:
                active_plugins.append(importlib.import_module("."+plugin,"resources.externalplugins").Plugin(dbservicefactory))
                logging.info("External plugin %s loaded"%(plugin))
            except Exception as exc:
                logging.error('%r failed to load. Exception:' % (plugin))
                traceback.print_exc()

    #main loop - execute the process() function of each plugin periodically            
    time_stamps = {}
    while(not time.sleep(1)):
        with ThreadPoolExecutor(max_workers = config.max_threads) as executor:
            
            futures = {}
            for pg in active_plugins:
                if (pg.get_name() in time_stamps):
                    if(time.time() - time_stamps[pg.get_name()] < pg.get_interval()):
                        continue
                futures[executor.submit(pg.process)]= pg.get_name()
                time_stamps[pg.get_name()] = time.time()
            
            for future in as_completed(futures):
                signal_name = futures[future]
                try:
                    value, payload = future.result()
                except Exception as exc:
                    logging.error('%r generated an exception:' % (signal_name))
                    traceback.print_exc()
                else:
                    generate_signal(signal_name,value.value,payload)