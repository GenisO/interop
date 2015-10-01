# encoding: utf-8
import time
import sys
import random
import json

# TODO: Which one?
from oauthlib import oauth1
from requests_oauthlib import OAuth1


thread_id = 0
num_threads = 1

node_server_id_dict = []
server_node_id_dict = []
server_folder_dict = defaultdict(list)
server_file_dict = defaultdict(list)

csv_timestamp = 0   # string
csv_normalized_timestamp = 1    # int
csv_user_id = 2     # int
csv_req_type = 3    # string
csv_node_id = 4     # int
csv_node_type = 5   # string or ""
csv_node_ext = 6    # "" or string
csv_size = 7        # NULL or num
csv_user_type = 8   # string

def event_dispatcher():
    previous_normalized_timestamp = 0
    with open("./traces/interop_all_ops.csv","r") as fp:
        for line in fp:
            event = line.split(',')
            print "%s %s" %(event[csv_timestamp], event[csv_user_id])
            if int(event[csv_user_id]) % num_threads == thread_id:
                # Process op
                t_sleep = int(event[csv_normalized_timestamp])-previous_normalized_timestamp
                print "Go to sleep for %s seconds" %(t_sleep)
                time.sleep(t_sleep)
                switcher = {
                    "GetContentResponse" : process_get,
                    "MakeResponse" : process_make,
                    "MoveResponse" : process_move,
                    "PutContentResponse" : process_put,
                    "Unlink" : process_delete,
                }
                # Get the function from switcher dictionary
                func = switcher.get(event[csv_op])
                func(event)
                previous_normalized_timestamp = int(event[csv_normalized_timestamp])

# TODO
def oauth(user_id):
    return OAuth1(CLIENT_KEY,
                    client_secret=CLIENT_SECRET,
                    resource_owner_key='Sl3UV1wBax51bkgrwiIeq79RRHJ5iI',
                    resource_owner_secret='cq4TCf6jcB8CadhmMXbqmOaO3crh1n')

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
            if server_id not in server_node_id_dict:
                server_node_id_dict[server_id] = node_id
            if is_folder and server_id not in server_folder_dict[user_id]:
                server_folder_dict[user_id].append(server_id)
            elif not is_folder and server_id not in server_file_dict[user_id]:
                server_file_dict[user_id].append(server_id)
        else raise ValueError("Error on response with status_code %d" %(response.status_code))
    except Exception as e:
        print "Exception at MakeResponse: node_id %s user_id %s with message %s" %(event_args[csv_node_id], event_args[csv_user_id], str(e))

def process_put(event_args):
    print "PutContentResponse node_id %s of user_id %s" %(event_args[csv_node_id], event_args[csv_user_id])
    user_id = event_args[csv_user_id]
    node_id = event_args[csv_node_id]
    # TODO: generate raw_data/file
    try:
        if node_id in node_server_id_dict:
            server_id = node_server_id_dict[node_id]
            if server_id not in server_file_dict[user_id]:
                if len(server_file_dict[user_id])>0:
                    server_id = random.sample(server_file_dict[user_id], 1)
                else:
                    raise ValueError("Error user %d does not have any file to update" %(user_id))
            response = put_content(oauth(user_id), server_id, local_path)
            if response.status_code == 200:
                if server_id not in server_file_dict[user_id]:
                    server_file_dict[user_id].append(server_id)
            else raise ValueError("Error on response with status_code %d" %(response.status_code))
        else:
            # TODO: send MakeResponse
    except Exception as e:
        print "Exception at PutContentResponse: node_id %s user_id %s with message %s" %(event_args[csv_node_id], event_args[csv_user_id], str(e))

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
                raise ValueError("Error user %d does not have any file to download" %(user_id))
            response = get_content(oauth(user_id), server_id)
            if response.status_code != 200:
                raise ValueError("Error on response with status_code %d" %(response.status_code))
        else:
            raise ValueError("Error user %d does not uploaded any file" %(user_id))
    except Exception as e:
        print "Exception at GetContentResponse: node_id %s user_id %s with message %s" %(event_args[csv_node_id], event_args[csv_user_id], str(e))

def process_delete(event_args):
    print "Unlink node_id %s of user_id %s" %(event_args[csv_node_id], event_args[csv_user_id])
    user_id = event_args[csv_user_id]
    node_id = event_args[csv_node_id]
    is_folder = event_args[csv_node_type] == "Directory"
    try:
        if user_id in server_file_dict:
            if node_id in node_server_id_dict:
                server_id = node_server_id_dict[node_id]



            elif len(server_file_dict[user_id])>0:
                server_id = random.sample(server_file_dict[user_id],1)
            else:
                raise ValueError("Error user %d does not have any file to delete" %(user_id))
            response = get_content(oauth(user_id), server_id)
            if response.status_code != 200:
                raise ValueError("Error on response with status_code %d" %(response.status_code))
        else:
            raise ValueError("Error user %d does not uploaded any file" %(user_id))
    except Exception as e:
        print "Exception at Unlink: node_id %s user_id %s with message %s" %(event_args[csv_node_id], event_args[csv_user_id], str(e))



def process_delete(event_args):
    user_id = event_args[csv_user_id]
    node_id = event_args[csv_node_id]
    is_folder = event_args[csv_node_type] == "Directory"
    if is_folder:
        if user_id in user_folder_dict:
            if node_id not in user_folder_dict[user_id]:
                if len(user_folder_dict[user_id])>0:
                    node_id = user_folder_dict[user_id].pop()
    else:
        user_file_dict[user_id].add(node_id)


    response = unlink(oauth(user_id), node_id, is_folder)
    if r.status_code == 200:
        user_file_dict[user_id].add(node_id)




def process_move(event_args):
    print "MoveResponse"




if __name__ == "__main__":
    # Launch main menu
    event_dispatcher()
