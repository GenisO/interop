# encoding: utf-8
import time
import sys
import random
import json
import os
import collections
import subprocess
import threading

from fake_api import *
from requests_oauthlib import OAuth1

thread_id = 0
num_threads = 1

node_server_id_dict = dict()
server_folder_dict = collections.defaultdict(list)
server_file_dict = collections.defaultdict(list)

csv_timestamp = 0
csv_normalized_timestamp = 1
csv_user_id = 2
csv_req_type = 3
csv_node_id = 4
csv_node_type = 5
csv_size = 6
csv_user_type = 7

user_oauth = collections.defaultdict(list)

oauth_client_key = 0
oauth_client_secret = 1
oauth_owner_key = 2
oauth_owner_secret = 3

class thread_trace_processor(threading.Thread):
    def run(self):
        event_dispatcher()
        return

def event_dispatcher():
    previous_normalized_timestamp = 0
    with open("./traces/interop_ops.csv","r") as fp:
        for line in fp:
            event = line.split(',')
            t_sleep = int(event[csv_normalized_timestamp])-previous_normalized_timestamp
            time.sleep(t_sleep)
            previous_normalized_timestamp = int(event[csv_normalized_timestamp])
            if int(event[csv_user_id]) % num_threads == thread_id:
                # Process op
                switcher = {
                    "GetContentResponse" : process_get,
                    "MakeResponse" : process_make,
                    "MoveResponse" : process_move,
                    "PutContentResponse" : process_put,
                    "Unlink" : process_delete,
                }
                # Get the function from switcher dictionary
                func = switcher.get(event[csv_req_type])
                func(event)
            user_input = raw_input("Some input please: ")

def oauth(user_id):
    return OAuth1(user_oauth[oauth_client_key],
                    client_secret=user_oauth[oauth_client_secret],
                    resource_owner_key=user_oauth[oauth_owner_key],
                    resource_owner_secret=user_oauth[oauth_owner_secret])

def process_make(event_args):
    print "MakeResponse node_id %s of user_id %s" %(event_args[csv_node_id], event_args[csv_user_id])
    user_id = event_args[csv_user_id]
    node_id = event_args[csv_node_id]
    is_folder = event_args[csv_node_type] == "Directory"
    try:
        response = make(oauth(user_id), node_id, is_folder)
        if response.status_code == 200:
            json_data = json.loads(response.text)
            server_id = json_data["id"]
            if node_id not in node_server_id_dict:
                node_server_id_dict[node_id] = server_id
            if is_folder and server_id not in server_folder_dict[user_id]:
                server_folder_dict[user_id].append(server_id)
            elif not is_folder and server_id not in server_file_dict[user_id]:
                server_file_dict[user_id].append(server_id)
        else:
            raise ValueError("Error on response with status_code %d" %(response.status_code))
    except Exception as e:
        print "Exception at MakeResponse: trace %s with error message %s" %(event_args, str(e))

def process_put(event_args):
    print "PutContentResponse node_id %s of user_id %s" %(event_args[csv_node_id], event_args[csv_user_id])
    user_id = event_args[csv_user_id]
    node_id = event_args[csv_node_id]
    size = event_args[csv_size]
    local_path = "./%s.file" %(thread_id)
    try:
        with open(local_path, "w") as f:
            subprocess.call(["fallocate", "-l", size, local_path])
        if node_id not in node_server_id_dict:
            event_args[csv_node_type] = "File"
            process_make(event_args)
        server_id = node_server_id_dict[node_id]
        if server_id not in server_file_dict[user_id]:
            if len(server_file_dict[user_id])>0:
                server_id = random.sample(server_file_dict[user_id], 1)
            else:
                raise ValueError("Error user %s does not have any file to update" %(user_id))
        response = put_content(oauth(user_id), server_id, local_path)
        if response.status_code == 200:
            if server_id not in server_file_dict[user_id]:
                server_file_dict[user_id].append(server_id)
        else:
            raise ValueError("Error on response with status_code %d" %(response.status_code))
    except Exception as e:
        print "Exception at PutContentResponse: trace %s with error message %s" %(event_args, str(e))
    finally:
        try:
            os.remove(local_path)
        except:
            pass

def process_get(event_args):
    print "GetContentResponse node_id %s of user_id %s" %(event_args[csv_node_id], event_args[csv_user_id])
    user_id = event_args[csv_user_id]
    node_id = event_args[csv_node_id]
    try:
        if user_id in server_file_dict:
            if node_id in node_server_id_dict:
                server_id = node_server_id_dict[node_id]
            elif len(server_file_dict[user_id])>0:
                server_id = random.sample(server_file_dict[user_id],1)
            else:
                raise ValueError("Error user %s does not have any file to download" %(user_id))
            response = get_content(oauth(user_id), server_id)
            if response.status_code != 200:
                raise ValueError("Error on response with status_code %d" %(response.status_code))
        else:
            raise ValueError("Error user %s does not uploaded any file" %(user_id))
    except Exception as e:
        print "Exception at GetContentResponse: trace %s with error message %s" %(event_args, str(e))

def process_delete(event_args):
    print "Unlink node_id %s of user_id %s" %(event_args[csv_node_id], event_args[csv_user_id])
    user_id = event_args[csv_user_id]
    node_id = event_args[csv_node_id]
    is_folder = event_args[csv_node_type] == "Directory"
    try:
        if is_folder:
            if user_id in server_folder_dict:
                if node_id in node_server_id_dict:
                    server_id = node_server_id_dict[node_id]
                elif len(server_folder_dict[user_id])>0:
                    server_id = random.sample(server_folder_dict[user_id],1)
                else:
                    raise ValueError("Error user %s does not have any folder to delete" %(user_id))
            else:
                raise ValueError("Error user %s does not uploaded any folder" %(user_id))
        else:
            if user_id in server_file_dict:
                if node_id in node_server_id_dict:
                    server_id = node_server_id_dict[node_id]
                elif len(server_file_dict[user_id])>0:
                    server_id = random.sample(server_file_dict[user_id],1)
                else:
                    raise ValueError("Error user %s does not have any file to delete" %(user_id))
            else:
                raise ValueError("Error user %s does not uploaded any file" %(user_id))
        response = unlink(oauth(user_id), server_id, is_folder)
        if response.status_code == 200:
            if is_folder:
                server_folder_dict[user_id].remove(server_id)
            else:
                server_file_dict[user_id].remove(server_id)
        else:
            raise ValueError("Error on response with status_code %d" %(response.status_code))
    except Exception as e:
        print "Exception at Unlink: trace %s with error message %s" %(event_args, str(e))

def process_move(event_args):
    print "MoveResponse node_id %s of user_id %s" %(event_args[csv_node_id], event_args[csv_user_id])
    user_id = event_args[csv_user_id]
    node_id = event_args[csv_node_id]
    is_folder = event_args[csv_node_type] == "Directory"
    try:
        if is_folder:
            if user_id in server_folder_dict:
                if node_id in node_server_id_dict:
                    server_id = node_server_id_dict[node_id]
                elif len(server_folder_dict[user_id])>0:
                    server_id = random.sample(server_folder_dict[user_id],1)
                else:
                    raise ValueError("Error user %s does not have any folder to move" %(user_id))
            else:
                raise ValueError("Error user %s does not uploaded any folder" %(user_id))
        else:
            if user_id in server_file_dict:
                if node_id in node_server_id_dict:
                    server_id = node_server_id_dict[node_id]
                elif len(server_file_dict[user_id])>0:
                    server_id = random.sample(server_file_dict[user_id],1)
                else:
                    raise ValueError("Error user %s does not have any file to move" %(user_id))
            else:
                raise ValueError("Error user %s does not uploaded any file" %(user_id))
        response = move(oauth(user_id), server_id, is_folder)
        if response.status_code != 200:
            raise ValueError("Error on response with status_code %d" %(response.status_code))
    except Exception as e:
        print "Exception at MoveResponse: trace %s with error message %s" %(event_args, str(e))

if __name__ == "__main__":
    t = thread_trace_processor()
    t.setDaemon(True)
    t.start()
    print "Thread started. Waiting"

    while t.isAlive():
        t.join(1)
