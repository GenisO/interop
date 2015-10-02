# encoding: utf-8
import sys
from requests_oauthlib import OAuth1
#from trace_processor import *
from API_manager import *
user_oauth = dict()

threads_pool = []

CLIENT_KEY = "b3af4e669daf880fb16563e6f36051b105188d413"
CLIENT_SECRET = "c168e65c18d75b35d8999b534a3776cf"




def create_users(user_file):
    #TODO:
    with open(user_file,"r") as fp:
        for user_id in fp:
            oauth = OAuth1(CLIENT_KEY,
                   client_secret=CLIENT_SECRET,
                   resource_owner_key='Sl3UV1wBax51bkgrwiIeq79RRHJ5iI',
                   resource_owner_secret='cq4TCf6jcB8CadhmMXbqmOaO3crh1n')
            user_oauth[user_id] = oauth

    user_oauth[0] = OAuth1(CLIENT_KEY,
           client_secret=CLIENT_SECRET,
           resource_owner_key="Sl3UV1wBax51bkgrwiIeq79RRHJ5iI",
           resource_owner_secret="cq4TCf6jcB8CadhmMXbqmOaO3crh1n")
    return

def run_threads_experiment(num_threads):
    for i in range(0, num_threads):
        t = thread_trace_processor(user_oauth, i, num_threads)
        t.setDaemon(True)
        threads_pool.append(t)
        t.start()

def run_experiment():
    event_dispatcher(oauth)

def wait_experiment():
    while len(threads_pool)>0:
        for t in threads_pool:
            t.join(1)
            if not t.isAlive():
                threads_pool.remove(t)

if __name__ == "__main__":
    create_users("./traces/interop_backup_users_id.csv")
    create_users("./traces/interop_cdn_users_id.csv")

    #run_threads_experiment(2)
    #run_experiment()
    #wait_experiment()

    print "MAKE"
    response = make(user_oauth[0], "file938293")
    print response
    print type(response)

    if response == 200:
        json_data = json.loads(response.text)
        server_id = json_data["id"]
        file_path = "./1.file"

        print "PUT"
        response = put_content(user_oauth[0], server_id, file_path)
        print response

        print "GET"
        response = get_content(user_oauth[0], server_id)
        print response

        print "MOVE"
        response = move(user_oauth[0], server_id)
        print response

        print "DELETE"
        response = unlink(user_oauth[0], server_id)
        print response
