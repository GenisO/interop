# encoding: utf-8
import sys
from requests_oauthlib import OAuth1
from trace_processor import *
from API_manager import *
from algo_time import *

user_oauth = dict()

threads_pool = []

CLIENT_KEY = "b3af4e669daf880fb16563e6f36051b105188d413"
CLIENT_SECRET = "c168e65c18d75b35d8999b534a3776cf"



user_oauth = dict()
def create_users(user_file):
    #TODO:
    with open(user_file,"r") as fp:
        for user_id in fp:
            oauth = OAuth1(CLIENT_KEY,
                   client_secret=CLIENT_SECRET,
                   resource_owner_key='Sl3UV1wBax51bkgrwiIeq79RRHJ5iI',
                   resource_owner_secret='cq4TCf6jcB8CadhmMXbqmOaO3crh1n')
            user_oauth[int(user_id)] = oauth

def run_threads_experiment(num_threads):
    for i in range(0, num_threads):
        t = thread_trace_processor(user_oauth, i, num_threads)
        t.setDaemon(True)
        threads_pool.append(t)
        t.start()

def run_experiment():
    event_dispatcher(user_oauth)

def wait_experiment():
    while len(threads_pool)>0:
        for t in threads_pool:
            t.join(1)
            print "Waiting ."
            if not t.isAlive():
                threads_pool.remove(t)
    print ("Experiment has finished")

# def test_api():
#     print "MAKE"
#     response = make(user_oauth[0], "file851")
#     print response
#     print response.text
#
#     json_data = json.loads(response.text)
#     server_id = json_data["id"]
#     print server_id
#     file_path = "./1.file"
#
#     print "PUT"
#     response = put_content(user_oauth[0], server_id, file_path)
#     print response
#     print response.text
#
#     print "GET"
#     response = get_content(user_oauth[0], server_id)
#     print response
#     print response.text
#
#     print "MOVE"
#     response = move(user_oauth[0], 2176)
#     print response
#     print response.text
#
#     print "DELETE"
#     response = unlink(user_oauth[0], server_id)
#     print response
#     print response.text


if __name__ == "__main__":
    #create_users("./traces/interop_backup_users_id.csv")
    #create_users("./traces/interop_cdn_users_id.csv")
    try:
        create_users("./traces/test_users.csv")
        run_threads_experiment(2)
        wait_experiment()
    except (KeyboardInterrupt, SystemExit):
        print ("\nExperiment killed")
