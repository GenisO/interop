# encoding: utf-8
import json
import os
import time
import sys

from glob import glob
from decimal import Decimal
from trace_processor import TraceProcessor, User, process_debug_log
from API_manager import CLIENT_KEY, CLIENT_SECRET, OAuth1, list_content, unlink, make, put_content, get_content

threads_pool = []

users_dict = dict()


def print_seq_dots():
    sys.stdout.write('.')
    sys.stdout.flush()


def create_test_user(is_ss_provider=True):
    if is_ss_provider:
        # 604036065 SS
        oauth = OAuth1(CLIENT_KEY,
                client_secret=CLIENT_SECRET,
                resource_owner_key="VsspgI198E7Sfy1SMRaY15V0DIoxAP",
                resource_owner_secret="lw1GUeFNrJSlj96iqQX6a4I7XW2rXA")
        user = User(604036065, oauth, 0, "SS")
        users_dict[604036065] = user
    else:
        # 3138145410 NEC
        oauth = OAuth1(CLIENT_KEY,
            client_secret=CLIENT_SECRET,
            resource_owner_key="947ba60a-145d-40e5-8f4e-1d3a73c1a501",
            resource_owner_secret="df6acaa5-0822-4ad0-bf97-43827f549e2e")

        user = User(3138145410, oauth, 2816, "NEC")
        users_dict[3138145410] = user


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
                    # friends_num = int(array_line[6])
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


def run_threads_experiment(file_trace_path, generated_trace):
    TraceProcessor(file_trace_path, generated_trace)


def clean_environment(users_path):
    print "\nCleaning environment"
    read_users_info(users_path)

    for i, user_id in enumerate(users_dict):
        user = users_dict[user_id]
        is_ss_provider = user.provider == "SS"
        response = list_content(user.oauth, user.shared_folder_id, is_ss_provider)
        if response.status_code == 200:
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
        else:
            print i, user_id, response, response.text, response.content
        print


def test_api(current_path):
    is_ss_provider = False
    create_test_user(is_ss_provider)

    user = users_dict.popitem()[1]
    root = user.shared_folder_id

    oauth = user.oauth
    file_name = "file5"
    folder_name = "folder5"
    is_folder = True

    start = time.time()
    response = make(oauth, folder_name, parent_id=root, is_folder=is_folder, is_ss_provider=is_ss_provider)
    end = time.time()
    print "MAKE folder elapsed;", (end - start)
    if response.status_code == 201:
        json_data = json.loads(response.text)
        folder_id = str(json_data["id"])
    elif (response.status_code == 400 or response.status_code == 403) and "already" in response.text:
        response = list_content(oauth, parent_id=root, is_ss_provider=is_ss_provider)
        json_data = response.json()
        content_root = json_data["contents"]
        folder_id = None
        for tuppla in content_root:
            try:
                name = tuppla["filename"]
                is_response_folder = tuppla["is_folder"]
                if name == folder_name and is_folder == is_response_folder:
                    folder_id = tuppla["id"]
                    break
            except KeyError:
                raise ValueError("ERROR MAKE Folder: Failed to extract folder_id form get_content at %s" % (response.url))
    else:
        raise ValueError(
            "ERROR MAKE Folder: response with status_code %d and text {%s}" % (response.status_code, response.text))
    print "Folder_id=", folder_id

    is_folder = False
    start = time.time()
    response = make(oauth, file_name, parent_id=folder_id, is_folder=is_folder, is_ss_provider=is_ss_provider)
    end = time.time()
    print "MAKE file elapsed;", (end - start)
    if response.status_code == 201:
        json_data = json.loads(response.text)
        server_id = str(json_data["id"])
    elif (response.status_code == 400 or response.status_code == 403) and "already" in response.text:
        response = list_content(oauth, parent_id=root, is_ss_provider=is_ss_provider)
        json_data = response.json()
        content_root = json_data["contents"]
        server_id = None
        for tuppla in content_root:
            try:
                name = tuppla["filename"]
                is_response_folder = tuppla["is_folder"]
                if name == folder_name and is_folder == is_response_folder:
                    server_id = tuppla["id"]
                    break
            except KeyError:
                raise ValueError("ERROR MAKE File: Failed to extract file_id form get_content at %s" % (response.url))
    else:
        raise ValueError(
            "ERROR MAKE File: response with status_code %d and text {%s}" % (response.status_code, response.text))
    print "File_id", server_id

    file_path = current_path + "/../README.md"
    start = time.time()
    response = put_content(oauth, server_id, file_path, is_ss_provider=is_ss_provider)
    end = time.time()
    print "PUT file elapsed;", (end - start)
    if not (response.status_code == 200 or response.status_code == 201):
        raise ValueError(
            "ERROR PUT File: response with status_code %d and text {%s}" % (response.status_code, response.text))

    start = time.time()
    response = get_content(oauth, server_id, is_ss_provider=is_ss_provider)
    end = time.time()
    print "GET file elapsed;", (end - start)
    if response.status_code != 200:
        raise ValueError(
            "ERROR GET File: response with status_code %d and text {%s}" % (response.status_code, response.text))

    start = time.time()
    response = unlink(oauth, server_id, is_folder=False, is_ss_provider=is_ss_provider)
    end = time.time()
    print "DELETE file elapsed;", (end - start)
    if response.status_code != 200:
        raise ValueError(
            "ERROR DELETE File: response with status_code %d and text {%s}" % (response.status_code, response.text))

    start = time.time()
    response = unlink(oauth, folder_id, is_folder=True, is_ss_provider=is_ss_provider)
    end = time.time()
    print "DELETE folder elapsed;", (end - start)
    if response.status_code != 200:
        raise ValueError(
            "ERROR DELETE Folder: response with status_code %d and text {%s}" % (response.status_code, response.text))


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
    # file_users_path = script_path + "/../traces/mini_users_full_interop_info.csv"
    # file_users_path = script_path + "/../target/cpd/nec_users_credentials_server_id_def.csv"
    # file_trace_path = script_path + "/../traces/interop_ops_without_moves_nanoseconds_accuracy.csv"

    # Local
    # file_users_path = script_path + "/../target/test/ast3_users_credentials_server_id.csv"
    # file_trace_path = script_path + "/../target/ast3_ops_norm.csv"
    # file_trace_path = script_path + "/../traces/interop_ops_without_moves_nanoseconds_accuracy.csv"


    # Mini interop
    file_users_path = script_path + "/../target/mini_test/new_mini_users_full_interop_info.csv"
    file_trace_path = script_path + "/../traces/interop_ops_without_moves_nanoseconds_accuracy.csv"
    file_generated_trace_path = script_path + "/../traces/new_generated_trace.csv"
    try:
        argv_list = sys.argv

        if len(argv_list) != 2:
            print_usage()
        else:
            if argv_list[1] == "run":
                # Run experiment
                read_users_info(file_users_path)
                run_threads_experiment(file_trace_path, file_generated_trace_path)
                # clean_environment(file_users_path)
            elif argv_list[1] == "clean":
                clean_environment(file_users_path)
            elif argv_list[1] == "test":
                test_api(script_path)
            elif argv_list[1] == "interop":
                read_users_info(script_path)
            else:
                print "ERROR: Option %s is not permitted" % (argv_list[1])
                print_usage()
                raise KeyboardInterrupt()

    except (KeyboardInterrupt, SystemExit):
        try:
            os.remove(glob("./*.file"))
        except:
            pass
        print ("\nExperiment killed")
        process_debug_log("Experiment killed")

