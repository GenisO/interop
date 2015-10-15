# encoding: utf-8
from collections import defaultdict
import sys
from API_manager import *
from requests_oauthlib import OAuth1
from trace_processor import thread_trace_processor, User
from random import random
from decimal import *

user_oauth = dict()

threads_pool = []

CLIENT_KEY = "b3af4e669daf880fb16563e6f36051b105188d413"
CLIENT_SECRET = "c168e65c18d75b35d8999b534a3776cf"

users_dict = dict()

def print_seq_dots():
    sys.stdout.write('.')
    sys.stdout.flush()


def create_test_user():
    oauth = OAuth1(CLIENT_KEY,
                   client_secret=CLIENT_SECRET,
                   resource_owner_key='sMuUlMeptCOHmXoefADVZQFSG9HOQH',
                   resource_owner_secret='yGtFhGjKnSYLfoBLDjinW40UyrImn8')
    user_oauth[0] = oauth


def read_users(user_file):
    # TODO: Make proper authentication
    #user_id,owner_key,owner_secret,shared_folder_id,provider,friends_num,user2,factor2,user3,factor3,...
    with open(user_file, "r") as fp:
        for i, line in enumerate(fp):
            if i > 1:
                array_line = line.rstrip('\n').split(",")

                user_id = int(array_line[0])
                owner_key = str(array_line[1])
                owner_secret = str(array_line[2])
                shared_folder_id = int(array_line[3])
                provider = str(array_line[4])
                friends_num = int(array_line[5])
                list_size = len(array_line)

                friends_dict = dict()
                for j in range(6, list_size, 2):
                    friend_id = int(array_line[j])
                    friend_factor = Decimal(str(array_line[j+1]))
                    friends_dict[friend_id] = friend_factor

                oauth = OAuth1(CLIENT_KEY,
                           client_secret=CLIENT_SECRET,
                           resource_owner_key=owner_key,
                           resource_owner_secret=owner_secret)

                user = User(user_id, oauth, shared_folder_id, provider, friends_dict)
                users_dict[user_id] = user


def run_threads_experiment(num_threads, file_trace_path):
    print "\nStarting experiment with %d threads" % (num_threads)
    for i in range(0, num_threads):
        t = thread_trace_processor(i, num_threads, file_trace_path, users_dict)
        t.setDaemon(True)
        threads_pool.append(t)
        t.start()


def wait_experiment():
    print "\nWaiting ",
    while len(threads_pool) > 0:
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
        except KeyError:
            pass
    print


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
    print "\tpython %s <option>" % (argv_list[0])
    print "Where options are:"
    print "\t[clean] delete all files at server side"
    print "\t[list] list content at root level"
    print "\t[test] runs small api test"
    print "\t[number] that represents how many threads will use"


if __name__ == "__main__":
    script_path = __file__[:__file__.rfind("/")]
    file_users_path = script_path + "/../traces/test_users.csv"
    file_trace_path = script_path + "/../traces/test_ops.csv"
    print __file__
    try:
        argv_list = sys.argv

        if len(argv_list) != 2:
            print_usage()
        else:
            try:
                num_threads = int(argv_list[1])
                read_users(file_users_path)
                run_threads_experiment(num_threads, file_trace_path)
                wait_experiment()
            except ValueError:
                if argv_list[1] == "clean":
                    clean_environment()
                elif argv_list[1] == "list":
                    create_test_user()
                    response = list_root_content(user_oauth[0])
                    print response.text
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
                elif argv_list[1] == "interop":
                    read_users(script_path)
                else:
                    print "ERROR: Option %s is not permitted" % (argv_list[1])
                    print_usage()

    except (KeyboardInterrupt, SystemExit):
        print ("\nExperiment killed")

