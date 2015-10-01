# encoding: utf-8
import time
import sys

# TODO: Which one?
from oauthlib import oauth1
from requests_oauthlib import OAuth1


thread_id = 0
num_threads = 1
user_node_id_dict = defaultdict(list)

csv_timestamp = 0
csv_normalized_timestamp = 1
csv_user_id = 2
csv_req_type = 3
csv_node_id = 4
csv_node_type = 5
csv_node_ext = 6
csv_size = 7
csv_user_type = 8

def event_dispatcher():
    previous_normalized_timestamp = 0
    with open('./traces/interop_all_ops.csv') as fp:
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

def process_get(event_args):
    print "GetContentResponse node_id %s of user_id %s" %(event_args[csv_node_id], event_args[csv_user_id])
    user_id = event_args[csv_user_id]
    node_id = event_args[csv_node_id]
    if user_id in user_node_id_dict:
        if node_id in user_node_id_dict[user_id]:
            get_content(oauth(user_id), node_id)
        elif len(user_node_id_dict[user_id]) > 0:
            get_content(oauth(user_id), user_node_id_dict[0])

def process_put(event_args):
    print "PutContentResponse node_id %s of user_id %s" %(event_args[csv_node_id], event_args[csv_user_id])
    user_id = event_args[csv_user_id]
    node_id = event_args[csv_node_id]
    if user_id in user_node_id_dict:
        if node_id in user_node_id_dict[user_id]:
            put_content(oauth(user_id), node_id)
        elif len(user_node_id_dict[user_id]) > 0:
            put_content(oauth(user_id), user_node_id_dict[0])

def process_make(event_args):
    print "MakeResponse"

def process_move(event_args):
    print "MoveResponse"

def process_delete(event_args):
    print "Unlink"

def process_error(event_args):
    print "An error occurred when parsing operation with timestamp %s and user_id %s" %(event_args[csv_timestamp], event_args[csv_user_id])
    sys.exit()





if __name__ == "__main__":
    # Launch main menu
    event_dispatcher()
