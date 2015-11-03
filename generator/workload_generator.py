# encoding: utf-8
import time
from API_manager import *
from requests_oauthlib import OAuth1
from trace_processor import TraceProcessor, User
from decimal import *

user_oauth = dict()

threads_pool = []

users_dict = dict()


def print_seq_dots():
    sys.stdout.write('.')
    sys.stdout.flush()


def create_test_user():
    # 3585146880,SS,39780,39795
    oauth = OAuth1(CLIENT_KEY,
                   client_secret=CLIENT_SECRET,
                   resource_owner_key="VABWlXtTGLnjsE4rLRCIGsse1d2Tie",
                   resource_owner_secret="YYuFVJIPsOqcoQ8QXGkWRTv3M4ZGJs")
    user_oauth[0] = oauth


def read_users_info(user_file):
    # user_id, owner_key, owner_secret, provider, shared_folder_id, file0_id, friends_num, user2, factor2, user3, factor3, ...
    with open(user_file, "r") as fp:
        for i, line in enumerate(fp):
            if i > 0:
                array_line = line.rstrip('\n').split(",")

                if array_line[0] != "":
                    user_id = int(array_line[0])
                    owner_key = str(array_line[1])
                    owner_secret = str(array_line[2])
                    provider = str(array_line[3])
                    shared_folder_id = str(array_line[4])
                    file0_id = str(array_line[5])
                    friends_num = int(array_line[6])
                    list_size = len(array_line)

                    friends_dict = dict()
                    for j in range(7, list_size, 2):
                        friend_id = int(array_line[j])
                        friend_factor = Decimal(str(array_line[j + 1]))
                        friends_dict[friend_id] = friend_factor

                    oauth = OAuth1(CLIENT_KEY,
                                   client_secret=CLIENT_SECRET,
                                   resource_owner_key=owner_key,
                                   resource_owner_secret=owner_secret)
                    # User(user_id, oauth, shared_folder_id, provider, friends_id_factor_dict=dict(), file0_id=None)
                    user = User(user_id, oauth, shared_folder_id, provider, friends_dict, file0_id)
                    users_dict[user_id] = user


def run_threads_experiment(file_trace_path):
    # print "\nStarting experiment with %d threads" % (num_threads)
    # for i in range(0, num_threads):
    #     t = ThreadTraceProcessor(i, num_threads, file_trace_path)
    #     t.setDaemon(True)
    #     threads_pool.append(t)
    #     t.start()
    processor = TraceProcessor(file_trace_path)


def wait_experiment():
    print "\nWaiting ",
    while len(threads_pool) > 0:
        for t in threads_pool:
            t.join(1)
            print_seq_dots()
            if not t.isAlive():
                threads_pool.remove(t)
    print ("\nExperiment has finished")


def clean_environment(users_path):
    print "\nCleaning environment"
    read_users_info(users_path)

    for i, user_id in enumerate(users_dict):
        user = users_dict[user_id]
        is_ss_provider = user.provider == "SS"
        response = list_content(user.oauth, user.shared_folder_id, is_ss_provider)
        json_data = response.json()
        content_root = json_data["contents"]
        print i, " ", user_id
        for line in content_root:
            try:
                print_seq_dots()
                name = line["filename"]
                server_id = line["id"]
                is_folder = line["is_folder"]
                if name.isdigit():
                    response = unlink(user.oauth, server_id, is_folder, is_ss_provider)
                    if response.status_code != 200:
                        print response, response.text, response.content
            except KeyError:
                pass
        print


def test_api(path):
    create_test_user()
    # shared_ss dir -> server_id = 9472
    is_ss_provider = True
    print "MAKE"
    start = time.time()
    response = make(user_oauth[0], "151548", parent_id=39780, is_folder=False, is_ss_provider=is_ss_provider)
    end = time.time()
    print "MAKE file", int(end-start)
    print response
    print response.headers
    print response.text
    print response.url
    print response.links
    json_data = json.loads(response.text)
    server_id = json_data["id"]
    print server_id

    file_path = path + "/../README.md"
    print "PUT"
    response = put_content(user_oauth[0], server_id, file_path, is_ss_provider=is_ss_provider)
    print response
    print response.text

    print "GET"
    response = get_content(user_oauth[0], server_id, is_ss_provider=is_ss_provider)
    print response
    print response.headers
    print response.text

    # print "MOVE"
    # response = move(user_oauth[0], server_id, is_ss_provider=False)
    # print response
    # print response.text

    print "DELETE"
    response = unlink(user_oauth[0], server_id, is_ss_provider=False)
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
    # # Rack
    # file_users_path = script_path + "/../target/cpd/users_full_interop_info.csv"
    # file_trace_path = script_path + "/../traces/interop_ops_without_moves.csv"

    # Local
    file_users_path = script_path + "/../target/ast3_full_interop_info.csv"
    file_trace_path = script_path + "/../target/ast3_ops.csv"
    print __file__
    try:
        argv_list = sys.argv

        if len(argv_list) != 2:
            print_usage()
        else:
            if argv_list[1] == "run":
                # Run experiment
                read_users_info(file_users_path)
                run_threads_experiment(file_trace_path)
                # wait_experiment()
            elif argv_list[1] == "clean":
                clean_environment(file_users_path)
            elif argv_list[1] == "list":
                create_test_user()
                folder_id = raw_input("Folder id: ")
                response = list_content(user_oauth[0], parent_id=folder_id, is_ss_provider=False)
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
                test_api(script_path)
            elif argv_list[1] == "interop":
                read_users_info(script_path)
            else:
                print "ERROR: Option %s is not permitted" % (argv_list[1])
                print_usage()

    except (KeyboardInterrupt, SystemExit):
        print ("\nExperiment killed")
