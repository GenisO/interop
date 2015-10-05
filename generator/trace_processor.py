# encoding: utf-8
import time
import sys
import random
import json
import os
import collections
import subprocess
import threading
import logging

from API_manager import *
from requests_oauthlib import OAuth1

class thread_trace_processor(threading.Thread):
    logging.basicConfig(filename='interop_experiment.log',
                    level=logging.DEBUG,
                    format='[%(levelname)s] (%(threadName)-10s) %(message)s',
                    )
    def __init__(self, p_user_oauth, p_thread_num, p_total_threads):
        threading.Thread.__init__(self)

        self.node_server_id_dict = dict()
        self.server_folder_dict = collections.defaultdict(list)
        self.server_file_dict = collections.defaultdict(list)

        self.csv_timestamp = 0
        self.csv_normalized_timestamp = 1
        self.csv_user_id = 2
        self.csv_req_type = 3
        self.csv_node_id = 4
        self.csv_node_type = 5
        self.csv_size = 6
        self.csv_user_type = 7

        self.user_oauth = p_user_oauth
        self.thread_id = p_thread_num
        self.num_threads = p_total_threads

        logging.debug("Thread_id %d: __init__ %d:%d" %(self.thread_id, self.thread_id, self.num_threads))

    def run(self):
        self.event_dispatcher()

    def event_dispatcher(self):
        logging.debug("Thread %d of %d event_dispatcher" %(self.thread_id, self.num_threads))
        previous_normalized_timestamp = 0
        with open("./traces/test_ops.csv","r") as fp:
            for line in fp:
                event = line.split(',')
                t_sleep = int(event[self.csv_normalized_timestamp])-previous_normalized_timestamp
                logging.debug("Thread_id %d: going to sleep %s s" %(self.thread_id, t_sleep))
                time.sleep(t_sleep)
                previous_normalized_timestamp = int(event[self.csv_normalized_timestamp])
                if int(event[self.csv_user_id]) % self.num_threads == self.thread_id:
                    # Process op
                    switcher = {
                        "GetContentResponse" : self.process_get,
                        "MakeResponse" : self.process_make,
                        "MoveResponse" : self.process_move,
                        "PutContentResponse" : self.process_put,
                        "Unlink" : self.process_delete,
                    }
                    # Get the function from switcher dictionary
                    func = switcher.get(event[self.csv_req_type])
                    func(event)
        logging.debug("Thread_id %d: Exiting" %(self.thread_id))

    def oauth(self, user_id):
        if int(user_id) not in self.user_oauth:
            raise ValueError("Error no oauth for user %s" %(user_id))
        return self.user_oauth[int(user_id)]

    def process_make(self, event_args):
        logging.debug("Thread %d MakeResponse node_id %d of user_id %d" %(self.thread_id, int(event_args[self.csv_node_id]), int(event_args[self.csv_user_id])))
        user_id = int(event_args[self.csv_user_id])
        node_id = int(event_args[self.csv_node_id])
        is_folder = event_args[self.csv_node_type] == "Directory"
        try:
            response = make(self.oauth(user_id), node_id, is_folder)
            if response.status_code == 201:
                json_data = json.loads(response.text)
                server_id = int(json_data["id"])
                if node_id not in self.node_server_id_dict:
                    self.node_server_id_dict[node_id] = server_id
                if is_folder and server_id not in self.server_folder_dict[user_id]:
                    self.server_folder_dict[user_id].append(server_id)
                elif not is_folder and server_id not in self.server_file_dict[user_id]:
                    self.server_file_dict[user_id].append(server_id)
            else:
                raise ValueError("Error on response with status_code  %d and text %s" %(response.status_code, response.text))))
        except Exception as e:
            logging.debug("Thread %d Exception at MakeResponse: trace %s. Error Description: %s {%s}" %(self.thread_id, event_args, e.message, e.args))

    def process_put(self, event_args):
        logging.debug("Thread %d PutContentResponse node_id %d of user_id %d" %(self.thread_id, int(event_args[self.csv_node_id]), int(event_args[self.csv_user_id])))
        user_id = int(event_args[self.csv_user_id])
        node_id = int(event_args[self.csv_node_id])
        size = event_args[self.csv_size]
        local_path = "./%s.file" %(self.thread_id)
        try:
            if node_id not in self.node_server_id_dict:
                event_args[self.csv_node_type] = "File"
                self.process_make(event_args)
            server_id = self.node_server_id_dict[node_id]
            if server_id not in self.server_file_dict[user_id]:
                if len(self.server_file_dict[user_id])>0:
                    server_id = random.sample(self.server_file_dict[user_id], 1)
                else:
                    raise ValueError("Error user %s does not have any file to update" %(user_id))
            with open(local_path, "w") as f:
                subprocess.call(["fallocate", "-l", size, local_path])
            response = put_content(self.oauth(user_id), server_id, local_path)
            if response.status_code == 200:
                if server_id not in self.server_file_dict[user_id]:
                    self.server_file_dict[user_id].append(server_id)
            else:
                raise ValueError("Error on response with status_code  %d and text %s" %(response.status_code, response.text))))
        except Exception as e:
            logging.debug("Thread %d Exception at PutContentResponse: trace %s. Error Description: %s {%s}" %(self.thread_id, event_args, e.message, e.args))
        finally:
            try:
                os.remove(local_path)
            except:
                pass

    def process_get(self, event_args):
        logging.debug("Thread %d GetContentResponse node_id %d of user_id %d" %(self.thread_id, int(event_args[self.csv_node_id]), int(event_args[self.csv_user_id])))
        user_id = int(event_args[self.csv_user_id])
        node_id = int(event_args[self.csv_node_id])
        try:
            if user_id in self.server_file_dict:
                if node_id in self.node_server_id_dict:
                    server_id = self.node_server_id_dict[node_id]
                elif len(self.server_file_dict[user_id])>0:
                    server_id = random.sample(self.server_file_dict[user_id],1)
                else:
                    raise ValueError("Error user %s does not have any file to download" %(user_id))
                response = get_content(self.oauth(user_id), server_id)
                if response.status_code != 200:
                    raise ValueError("Error on response with status_code %d" %(response))
            else:
                raise ValueError("Error user %s does not uploaded any file" %(user_id))
        except Exception as e:
            logging.debug("Thread %d Exception at GetContentResponse: trace %s. Error Description: %s {%s}" %(self.thread_id, event_args, e.message, e.args))

    def process_delete(self, event_args):
        logging.debug("Thread %d Unlink node_id %d of user_id %d" %(self.thread_id, int(event_args[self.csv_node_id]), int(event_args[self.csv_user_id])))
        user_id = int(event_args[self.csv_user_id])
        node_id = int(event_args[self.csv_node_id])
        is_folder = event_args[self.csv_node_type] == "Directory"
        try:
            if is_folder:
                if user_id in self.server_folder_dict:
                    if node_id in self.node_server_id_dict:
                        server_id = self.node_server_id_dict[node_id]
                    elif len(self.server_folder_dict[user_id])>0:
                        server_id = random.sample(self.server_folder_dict[user_id],1)
                    else:
                        raise ValueError("Error user %s does not have any folder to delete" %(user_id))
                else:
                    raise ValueError("Error user %s does not uploaded any folder" %(user_id))
            else:
                if user_id in self.server_file_dict:
                    if node_id in self.node_server_id_dict:
                        server_id = self.node_server_id_dict[node_id]
                    elif len(self.server_file_dict[user_id])>0:
                        server_id = random.sample(self.server_file_dict[user_id],1)
                    else:
                        raise ValueError("Error user %s does not have any file to delete" %(user_id))
                else:
                    raise ValueError("Error user %s does not uploaded any file" %(user_id))
            response = unlink(self.oauth(user_id), server_id, is_folder)
            if response.status_code == 200:
                if is_folder:
                    self.server_folder_dict[user_id].remove(server_id)
                else:
                    self.server_file_dict[user_id].remove(server_id)
            else:
                raise ValueError("Error on response with status_code  %d and text %s" %(response.status_code, response.text))))
        except Exception as e:
            logging.debug("Thread %d Exception at Unlink: trace %s. Error Description: %s {%s}" %(self.thread_id, event_args, e.message, e.args))

    def process_move(self, event_args):
        logging.debug("Thread %d MoveResponse node_id %d of user_id %d" %(self.thread_id, int(event_args[self.csv_node_id]), int(event_args[self.csv_user_id])))
        user_id = int(event_args[self.csv_user_id])
        node_id = int(event_args[self.csv_node_id])
        is_folder = event_args[self.csv_node_type] == "Directory"
        try:
            if is_folder:
                if user_id in self.server_folder_dict:
                    if node_id in self.node_server_id_dict:
                        server_id = self.node_server_id_dict[node_id]
                    elif len(self.server_folder_dict[user_id])>0:
                        server_id = random.sample(self.server_folder_dict[user_id],1)
                    else:
                        raise ValueError("Error user %s does not have any folder to move" %(user_id))
                else:
                    raise ValueError("Error user %s does not uploaded any folder" %(user_id))
            else:
                if user_id in self.server_file_dict:
                    if node_id in self.node_server_id_dict:
                        server_id = self.node_server_id_dict[node_id]
                    elif len(self.server_file_dict[user_id])>0:
                        server_id = random.sample(self.server_file_dict[user_id],1)
                    else:
                        raise ValueError("Error user %s does not have any file to move" %(user_id))
                else:
                    raise ValueError("Error user %s does not uploaded any file" %(user_id))
            response = move(self.oauth(user_id), server_id, is_folder)
            if response.status_code != 200:
                raise ValueError("Error on response with status_code %d and text %s" %(response.status_code, response.text))
        except Exception as e:
            logging.debug("Thread %d Exception at MoveResponse: trace %s. Error Description: %s {%s}" %(self.thread_id, event_args, e.message, e.args))

if __name__ == "__main__":
    print "Error: This class must be instantiated"
