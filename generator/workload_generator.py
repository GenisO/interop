# encoding: utf-8
import sys
from API_manager import *
from requests_oauthlib import OAuth1
from trace_processor import thread_trace_processor

user_oauth = dict()

threads_pool = []

CLIENT_KEY = "b3af4e669daf880fb16563e6f36051b105188d413"
CLIENT_SECRET = "c168e65c18d75b35d8999b534a3776cf"

user_oauth = dict()

def print_seq_dots():
    sys.stdout.write('.')
    sys.stdout.flush()

def create_test_user():
    oauth = OAuth1(CLIENT_KEY,
           client_secret=CLIENT_SECRET,
           resource_owner_key='Sl3UV1wBax51bkgrwiIeq79RRHJ5iI',
           resource_owner_secret='cq4TCf6jcB8CadhmMXbqmOaO3crh1n')
    user_oauth[0] = oauth

def create_users(user_file):
    #TODO: Make proper authentication
    with open(user_file,"r") as fp:
        for user_id in fp:
            oauth = OAuth1(CLIENT_KEY,
                   client_secret=CLIENT_SECRET,
                   resource_owner_key='Sl3UV1wBax51bkgrwiIeq79RRHJ5iI',
                   resource_owner_secret='cq4TCf6jcB8CadhmMXbqmOaO3crh1n')
            user_oauth[int(user_id)] = oauth

def run_threads_experiment(num_threads):
    print "\nStarting experiment with %d threads" %(num_threads)
    for i in range(0, num_threads):
        t = thread_trace_processor(user_oauth, i, num_threads)
        t.setDaemon(True)
        threads_pool.append(t)
        t.start()

def run_experiment():
    event_dispatcher(user_oauth)

def wait_experiment():
    print "\nWaiting ",
    while len(threads_pool)>0:
        for t in threads_pool:
            t.join(1)
            print_seq_dots()
            if not t.isAlive():
                threads_pool.remove(t)
    print ("\nExperiment has finished")


def clean_environment():
    print "\nCleaning environment"
    create_test_user()
    response = list_root_content(user_oauth[0])
    json_data = response.json()
    content_root = json_data["contents"]
    for line in content_root:
        try:
            print_seq_dots()
            name = line["filename"]
            server_id = line["id"]
            is_folder = line["is_folder"]
            if name.isdigit():
                response = unlink(user_oauth[0], server_id, is_folder)
        except KeyError as e:
            pass
    print

def load_initial_environment():
    print "\nLoading defaults files"
    for user_id in user_oauth:
        print_seq_dots()
        response = make(user_oauth[user_id], user_id)

def test_api():
    create_test_user()
    print "MAKE"
    response = make(user_oauth[0], "15796961")
    print response
    print response.headers
    print response.text
    print response.url
    print response.links
    json_data = json.loads(response.text)
    server_id = json_data["id"]
    print server_id

    file_path = "./README.md"
    print "PUT"
    response = put_content(user_oauth[0], server_id, file_path)
    print response

    print "GET"
    response = get_content(user_oauth[0], server_id)
    print response
    print response.headers
    print response.text

    print "MOVE"
    response = move(user_oauth[0], server_id)
    print response
    print response.text

    print "DELETE"
    response = unlink(user_oauth[0], server_id)
    print response
    print response.text

def print_usage():
    print "USAGE ERROR:"
    print "\tpyton %s <option>" %(argv_list[0])
    print "Where options are:"
    print "\t[clean] delete all files at server side"
    print "\t[list] list content at root level"
    print "\t[test] runs small api test"
    print "\t[number] that represents how many threads will use"

if __name__ == "__main__":
    try:
        argv_list = sys.argv

        if len(argv_list)!=2:
            print_usage()
        else:
            try:
                num_threads = int(argv_list[1])
                # create_users("./traces/test_users.csv")
                # load_initial_environment()
                create_users("./traces/interop_backup_users_id.csv")
                create_users("./traces/interop_cdn_users_id.csv")
                run_threads_experiment(num_threads)
                wait_experiment()
            except ValueError:
                if argv_list[1] == "clean":
                    clean_environment()
                elif argv_list[1] == "list":
                    create_test_user()
                    response = list_root_content(user_oauth[0])
                    json_data = response.json()
                    content_root = json_data["contents"]
                    for line in content_root:
                        try:
                            name = line["filename"]
                            server_id = line["id"]
                            is_folder = line["is_folder"]
                            status = line["status"]
                            print name, server_id, is_folder, status
                        except:
                            pass
                elif argv_list[1] == "test":
                    test_api()
                else:
                    print "ERROR: Option %s is not permitted" %(argv_list[1])
                    print_usage()

    except (KeyboardInterrupt, SystemExit):
        print ("\nExperiment killed")
