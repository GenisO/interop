# encoding: utf-8
import time
import sys




thread_id = 0
num_threads = 1
users_node_dict = {}

csv_timestamp = 0
csv_normalized_timestamp = 1
csv_user_id = 2
csv_op = 3
csv_node_id = 4
csv_size = 5
csv_user_type = 6

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
                # user_input = raw_input("Some input please: ")
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


def process_get(event_args):
    print "GetContentResponse %s" %(event_args[csv_user_id])




def process_make(event_args):
    print "MakeResponse"

def process_move(event_args):
    print "MoveResponse"

def process_put(event_args):
    print "PutContentResponse"

def process_delete(event_args):
    print "Unlink"

def process_error(event_args):
    print "An error occurred when parsing operation with timestamp %s and user_id %s" %(event_args[csv_timestamp], event_args[csv_user_id])
    sys.exit()





if __name__ == "__main__":
    # Launch main menu
    event_dispatcher()
