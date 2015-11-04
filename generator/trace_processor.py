# encoding: utf-8
from apt.package import DeprecatedProperty
from collections import deque
from lib2to3.pgen2.grammar import op
import os
import time
import random
import subprocess
import threading
import logging
import logging.handlers
import copy
import decimal

from API_manager import *

# TODO: Only for testing
log_file_name = "/../interop_experiment.log"
# log_file_name = "/../interop_experiment_%d.log" % (int(time.time()))
log_file_trace_path = __file__[:__file__.rfind("/")] + log_file_name

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(log_file_trace_path, maxBytes=20000000, backupCount=10)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("[%(levelname)s];[%(thread)d];%(asctime)s;%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
random.seed(18)


def process_log(tstamp, user_out_id, user_out_type, req_t, origin_provider, destination_provider, user_in_id,
                node_id, node_type, size, elapsed, friends_number, error_msg):
    line = ("[TRACE];%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s" % (str(tstamp), str(user_out_id), str(user_out_type),
                                                                str(req_t), str(origin_provider),
                                                                str(destination_provider),
                                                                str(user_in_id), str(node_id),
                                                                str(node_type),
                                                                str(size), str(elapsed), str(friends_number),
                                                                str(error_msg)))
    logger.info(line)


def process_debug_log(message):
    logger.info("[DEBUG] - %s" % (str(message)))


def process_error_log(message):
    logger.error(str(message))


class User(object):
    def __str__(self):
        return str(self.id)

    def __init__(self, user_id, oauth, shared_folder_id, provider, friends_id_factor_dict=None, file0_id=None):
        self.id = user_id
        self.oauth = oauth
        self.shared_folder_id = shared_folder_id
        self.provider = provider
        if friends_id_factor_dict is None:
            self.friends_id_factor_dict = dict()
        else:
            self.friends_id_factor_dict = friends_id_factor_dict
        self.file0_id = file0_id

        self.workspace_folders = list()
        self.workspace_files = list()

        # self.workspace_folders.append(shared_folder_id)
        # self.workspace_files.append(file0_id)

        self.node_server_id_dict = dict()
        self.process_thread = None
        TraceProcessor.all_users_dict[self.id] = self


class TraceProcessor:
    csv_timestamp = 0
    csv_normalized_timestamp = 1
    csv_user_id = 2
    csv_req_type = 3
    csv_node_id = 4
    csv_node_type = 5
    csv_ext = 6
    csv_size = 7
    csv_user_type = 8
    csv_friend_id = 9
    csv_provider = 10

    all_users_dict = dict()
    processing_threads = dict()

    def __init__(self, p_trace_path, test=False):
        # threading.Thread.__init__(self)

        self.trace_path = p_trace_path
        self.test_concurrency = test

        process_log("tstamp", "user_out_id", "user_out_type", "req_t", "origin_provider", "destination_provider",
                    "user_in_id", "node_id", "node_type", "size", "elapsed", "friends_number", "error_msg")
        self.event_dispatcher()

    # deprecated
    def load_initial_parameters(self):
        for user_id in TraceProcessor.all_users_dict:
            if user_id % self.num_threads == self.thread_id:
                user = TraceProcessor.all_users_dict[user_id]
                user_provider = user.provider
                user.workspaces_oauth[user.id] = user.oauth

                for friend_id in user.friends_id_factor_dict:
                    friend = TraceProcessor.all_users_dict[friend_id]
                    if user_provider == friend.provider:
                        user.workspaces_oauth[friend.id] = user.oauth
                    elif user_provider == "NEC":
                        response = list_content(user.oauth, 0, False)
                        json_data = response.json()
                        content_root = json_data["contents"]
                        shared_with_me_id = None
                        for tuppla in content_root:
                            try:
                                name = tuppla["filename"]
                                is_folder = tuppla["is_folder"]
                                if name == "Shared with me" and is_folder:
                                    shared_with_me_id = tuppla["id"]
                                    break
                            except KeyError:
                                pass

                        if shared_with_me_id is None:
                            user.friends_id_factor_dict.pop(friend_id)
                        else:
                            response = list_content(user.oauth, shared_with_me_id, False)
                            json_data = response.json()
                            content_root = json_data["contents"]
                            user_shared_folder_id = None
                            for tuppla in content_root:
                                try:
                                    name = tuppla["filename"]
                                    is_folder = tuppla["is_folder"]
                                    # TODO: Only for testing
                                    if str(100) in name and is_folder:
                                        # if str(friend_id) in name and is_folder:
                                        user_shared_folder_id = tuppla["id"]
                                        break
                                except KeyError:
                                    pass

                            if user_shared_folder_id is None:
                                user.friends_id_factor_dict.pop(friend_id)
                            else:
                                response = list_content(user.oauth, user_shared_folder_id, False)
                                json_data = response.json()
                                content_root = json_data["contents"]
                                acces_oauth = None
                                # workspace = friend.shared_folder_id
                                for tuppla in content_root:
                                    try:
                                        name = tuppla["name"]
                                        token_key = tuppla["access_token_key"]
                                        token_secret = tuppla["access_token_secret"]
                                        resource_url = tuppla["resource_url"]

                                        if name == "shared_folder":
                                            acces_oauth = OAuth1(CLIENT_KEY,
                                                                 client_secret=CLIENT_SECRET,
                                                                 resource_owner_key=token_key,
                                                                 resource_owner_secret=token_secret)

                                            # workspace = resource_url[resource_url.rfind("/") + 1:]
                                            break
                                    except KeyError:
                                        pass

                                if acces_oauth is None:
                                    user.friends_id_factor_dict.pop(friend_id)
                                else:
                                    user.workspaces_oauth[friend_id] = acces_oauth

    def event_dispatcher(self):
        previous_normalized_timestamp = decimal.Decimal("0.00000000")
        previous_time = decimal.Decimal(time.time())

        # TODO: Only for testing
        if self.test_concurrency:
            self.prove_concurrency(previous_normalized_timestamp, previous_time)
        else:
            with open(self.trace_path, "r") as fp:
                for i, line in enumerate(fp):
                    event = line.rstrip("\n").split(",")
                    if len(event) == 9:
                        elapsed = decimal.Decimal(time.time()) - previous_time
                        t_sleep = decimal.Decimal(
                            event[TraceProcessor.csv_normalized_timestamp]) - previous_normalized_timestamp
                        # Control sleep time
                        t_sleep = t_sleep - elapsed
                        if t_sleep < 0:
                            t_sleep = decimal.Decimal("0.00000000")
                        time.sleep(t_sleep)
                        previous_normalized_timestamp = decimal.Decimal(
                            event[TraceProcessor.csv_normalized_timestamp])

                        user_id = int(event[TraceProcessor.csv_user_id])

                        if user_id in self.processing_threads:
                            t1 = self.processing_threads[user_id]
                            t1.add_event(i, event)
                            if not t1.running or not t1.isAlive():
                                t1.run()
                        else:
                            t1 = ThreadedPetition(user_id, i, event)
                            t1.setDaemon(True)
                            t1.start()
                            self.processing_threads[user_id] = t1

                        previous_time = decimal.Decimal(time.time())
                    else:
                        process_debug_log("Avoided line %s" % (line))
            self.wait_experiment()

    # TODO: Only for testing
    def prove_concurrency(self, previous_normalized_timestamp, previous_time):
        for total_ops in range(1, 50):
            for loop in range(0, int(total_ops / 16) + 1):
                with open(self.trace_path, "r") as fp:
                    for i, line in enumerate(fp):
                        event = line.rstrip("\n").split(",")
                        if len(event) == 9 and i + loop * 16 < total_ops:
                            user_id = int(event[TraceProcessor.csv_user_id])

                            if user_id in self.processing_threads:
                                t1 = self.processing_threads[user_id]
                                t1.add_event(i, event)
                                if not t1.running or not t1.isAlive():
                                    t1.run()
                            else:
                                t1 = ThreadedPetition(user_id, i, event)
                                t1.setDaemon(True)
                                t1.start()

                            previous_time = decimal.Decimal(time.time())
                        elif i + loop * 16 == total_ops:
                            break
                        else:
                            process_debug_log("Avoided line %s" % (line))
                self.wait_experiment()
                raw_input("Concurrent %d ops done! Press to next" % total_ops)
                print "\n"

    def wait_experiment(self):
        print "\nWaiting "
        wait = True
        while wait:
            wait = not wait
            for u in self.processing_threads:
                t = self.processing_threads[u]
                t.join(1)
                print_seq_dots()
                if t.isAlive():
                    wait = True
                    break
        print ("\nExperiment has finished")


def print_seq_dots():
    sys.stdout.write('.')
    sys.stdout.flush()


class ThreadedPetition(threading.Thread):
    def __init__(self, user, ops_counter, event):
        threading.Thread.__init__(self)
        self.running = False
        self.first_ops_counter = ops_counter
        self.user = user

        self.event_args = deque()
        self.event_args.append([ops_counter, event])

    def add_event(self, ops_counter, event):
        self.first_ops_counter = ops_counter
        self.event_args.append([ops_counter, event])

    def run(self):
        print "Thread start ", self.ident, " count ", self.first_ops_counter, "\n"
        self.running = True
        while len(self.event_args) > 0:
            [ops_counter, event] = self.event_args.popleft()
            # Process op
            switcher = {
                "GetContentResponse": self.process_get,
                "MakeResponse": self.process_make,
                "MoveResponse": self.process_move,
                "PutContentResponse": self.process_put,
                "Unlink": self.process_delete,
            }
            func = switcher.get(event[TraceProcessor.csv_req_type])
            self.preprocessor(ops_counter, func, event)

        self.running = False

    def preprocessor(self, ops_counter, func, event_args):
        user_id = int(event_args[TraceProcessor.csv_user_id])

        try:
            user = TraceProcessor.all_users_dict[user_id]
            if user.provider == "NEC":
                raise KeyError
        except KeyError:
            # TODO: Only for testing
            user = TraceProcessor.all_users_dict[3439236469]
            event_args = copy.deepcopy(event_args)
            event_args[TraceProcessor.csv_user_id] = str(3439236469)

        p = decimal.Decimal(str(random.random()))

        factors = user.friends_id_factor_dict.values()
        if len(factors) > 0:
            factors.sort()
            target = factors[-1]
            multiple = False
            for v in factors:
                if p < v:
                    target = v
                    break
                elif p == v:
                    target = v
                    multiple = True
                    break

            friends_id = []
            for u in user.friends_id_factor_dict:
                if user.friends_id_factor_dict[u] == target:
                    friends_id.append(u)
                    if not multiple:
                        break

            friend_id = random.sample(friends_id, 1)[0]
            friend = TraceProcessor.all_users_dict[friend_id]
        else:
            friend = TraceProcessor.all_users_dict[user_id]

        # TODO: Only for testing
        if friend.provider == "NEC":
            friend = user

        event_args.append(friend.id)  # csv_friend_id
        event_args.append(friend.provider)  # csv_provider

        func(event_args, ops_counter)

    def process_make(self, event_args, ops_counter):
        process_debug_log("Count %d ProcessMakeResponse node_id %d of user_id %s" % (ops_counter,
                                                                                     int(event_args[
                                                                                             TraceProcessor.csv_node_id]),
                                                                                     int(event_args[
                                                                                             TraceProcessor.csv_user_id])))

        user_id = int(event_args[TraceProcessor.csv_user_id])
        node_id = int(event_args[TraceProcessor.csv_node_id])
        is_folder = event_args[TraceProcessor.csv_node_type] == "Directory"
        friend_id = int(event_args[TraceProcessor.csv_friend_id])

        friend = TraceProcessor.all_users_dict[friend_id]
        workspace = friend.shared_folder_id
        oauth = friend.oauth
        is_ss_provider = friend.provider == "SS"
        start = time.time()
        try:
            start = time.time()
            response = make(oauth, node_id, workspace, is_folder, is_ss_provider)
            end = time.time()

            if response.status_code == 201:
                json_data = json.loads(response.text)
                server_id = str(json_data["id"])

                elapsed = end - start
                process_log(str(repr(start)), str(user_id), str(event_args[TraceProcessor.csv_user_type]),
                            str(event_args[TraceProcessor.csv_req_type]),
                            str(TraceProcessor.all_users_dict[user_id].provider),
                            str(TraceProcessor.all_users_dict[friend_id].provider), str(friend_id),
                            str(server_id),
                            str(event_args[TraceProcessor.csv_node_type]), "NULL", str(elapsed),
                            str(len(TraceProcessor.all_users_dict[user_id].friends_id_factor_dict)), "NULL")
            elif (response.status_code == 400 or response.status_code == 403) and "already" in response.text:
                response = list_content(oauth, parent_id=workspace, is_ss_provider=is_ss_provider)
                json_data = response.json()
                content_root = json_data["contents"]
                server_id = None
                for tuppla in content_root:
                    try:
                        name = tuppla["filename"]
                        is_response_folder = tuppla["is_folder"]
                        if name == str(node_id) and is_folder == is_response_folder:
                            server_id = tuppla["id"]
                            break
                    except KeyError:
                        pass

                process_log(str(repr(time.time())), str(user_id), str(event_args[TraceProcessor.csv_user_type]),
                            str(event_args[TraceProcessor.csv_req_type]),
                            str(TraceProcessor.all_users_dict[user_id].provider),
                            str(TraceProcessor.all_users_dict[friend_id].provider), str(friend_id),
                            str(server_id),
                            str(event_args[TraceProcessor.csv_node_type]), "NULL", "0",
                            str(len(TraceProcessor.all_users_dict[user_id].friends_id_factor_dict)), "NULL")
            else:
                raise ValueError(
                    "Error on response with status_code %d and text {%s}" % (response.status_code, response.text))

            if server_id is not None:
                if node_id not in friend.node_server_id_dict:
                    friend.node_server_id_dict[node_id] = server_id
                if is_folder and server_id not in friend.workspace_folders:
                    friend.workspace_folders.append(server_id)
                elif not is_folder and server_id not in friend.workspace_files:
                    friend.workspace_files.append(server_id)
        except Exception as e:
            error_msg = "Exception at MakeResponse: trace %s. Error Description: type=%s message={%s} args={%s}" % (
                event_args, type(e), e.message, e.args)
            process_error_log(error_msg)
            elapsed = time.time() - start
            process_log(str(repr(start)), str(user_id), str(event_args[TraceProcessor.csv_user_type]),
                        str(event_args[TraceProcessor.csv_req_type]),
                        str(TraceProcessor.all_users_dict[user_id].provider),
                        str(TraceProcessor.all_users_dict[friend_id].provider), str(friend_id), str(node_id),
                        str(event_args[TraceProcessor.csv_node_type]), "NULL", str(elapsed),
                        str(len(TraceProcessor.all_users_dict[user_id].friends_id_factor_dict)), error_msg)

    def process_put(self, event_args, ops_counter):
        process_debug_log("Count %d ProcessPutContentResponse node_id %d of user_id %s" % (ops_counter,
                                                                                           int(event_args[
                                                                                                   TraceProcessor.csv_node_id]),
                                                                                           int(event_args[
                                                                                                   TraceProcessor.csv_user_id])))

        user_id = int(event_args[TraceProcessor.csv_user_id])
        node_id = int(event_args[TraceProcessor.csv_node_id])
        friend_id = int(event_args[TraceProcessor.csv_friend_id])
        size = int(event_args[TraceProcessor.csv_size])

        friend = TraceProcessor.all_users_dict[friend_id]
        oauth = friend.oauth
        is_ss_provider = friend.provider == "SS"

        local_path = "./%s.file" % (self.ident)
        start = time.time()
        try:
            if node_id not in friend.node_server_id_dict:
                # mod_event_args = copy.deepcopy(event_args)
                # mod_event_args[TraceProcessor.csv_node_type] = "File"
                # mod_event_args[TraceProcessor.csv_req_type] = "MakeResponse"
                # self.process_make(mod_event_args, ops_counter)

                if len(friend.workspace_files) > 0:
                    server_id = random.sample(friend.workspace_files, 1)[0]
                else:
                    server_id = friend.file0_id
            else:
                server_id = friend.node_server_id_dict[node_id]

            if size < 1:
                size = 2
            with open(local_path, "w"):
                subprocess.check_call(["fallocate", "-l", str(size), local_path])

            start = time.time()
            response = put_content(oauth, server_id, local_path, is_ss_provider)
            end = time.time()

            if response.status_code == 200 or response.status_code == 201:
                elapsed = end - start
                if friend.provider == "NEC":
                    size = str(size)
                else:
                    json_data = json.loads(response.text)
                    size = str(json_data["size"])

                process_log(str(repr(start)), str(user_id), str(event_args[TraceProcessor.csv_user_type]),
                            str(event_args[TraceProcessor.csv_req_type]),
                            str(TraceProcessor.all_users_dict[user_id].provider),
                            str(TraceProcessor.all_users_dict[friend_id].provider), str(friend_id),
                            str(server_id),
                            str(event_args[TraceProcessor.csv_node_type]), str(size), str(elapsed),
                            str(len(TraceProcessor.all_users_dict[user_id].friends_id_factor_dict)), "NULL")
            else:
                raise ValueError(
                    "Error on response with status_code %d and text %s" % (response.status_code, response.text))
        except subprocess.CalledProcessError as e:
            error_msg = "Exception at fallocate with size %d: trace %s. Error Description: type=%s message={%s} args={%s}" % (
                size, event_args, type(e), e.message, e.args)
            process_error_log(error_msg)
            elapsed = time.time() - start
            process_log(str(repr(start)), str(user_id), str(event_args[TraceProcessor.csv_user_type]),
                        str(event_args[TraceProcessor.csv_req_type]),
                        str(TraceProcessor.all_users_dict[user_id].provider),
                        str(TraceProcessor.all_users_dict[friend_id].provider), str(friend_id), str(node_id),
                        str(event_args[TraceProcessor.csv_node_type]), "NULL", str(elapsed),
                        str(len(TraceProcessor.all_users_dict[user_id].friends_id_factor_dict)), error_msg)
        except Exception as e:
            error_msg = "Exception at PutContentResponse: trace %s. Error Description: type=%s message={%s} args={%s}" % (
                event_args, type(e), e.message, e.args)
            process_error_log(error_msg)
            elapsed = time.time() - start
            process_log(str(repr(start)), str(user_id), str(event_args[TraceProcessor.csv_user_type]),
                        str(event_args[TraceProcessor.csv_req_type]),
                        str(TraceProcessor.all_users_dict[user_id].provider),
                        str(TraceProcessor.all_users_dict[friend_id].provider), str(friend_id), str(node_id),
                        str(event_args[TraceProcessor.csv_node_type]), "NULL", str(elapsed),
                        str(len(TraceProcessor.all_users_dict[user_id].friends_id_factor_dict)), error_msg)
        finally:
            try:
                os.remove(local_path)
            except:
                pass

    def process_get(self, event_args, ops_counter):
        process_debug_log("Count %d ProcessGetContentResponse node_id %d of user_id %s" % (ops_counter,
                                                                                           int(event_args[
                                                                                                   TraceProcessor.csv_node_id]),
                                                                                           int(event_args[
                                                                                                   TraceProcessor.csv_user_id])))

        user_id = int(event_args[TraceProcessor.csv_user_id])
        node_id = int(event_args[TraceProcessor.csv_node_id])
        friend_id = int(event_args[TraceProcessor.csv_friend_id])

        friend = TraceProcessor.all_users_dict[friend_id]
        oauth = friend.oauth
        is_ss_provider = friend.provider == "SS"
        start = time.time()
        try:
            server_id = None
            if node_id in friend.node_server_id_dict:
                server_id = friend.node_server_id_dict[node_id]
            if server_id not in friend.workspace_files:
                if len(friend.workspace_files) > 0:
                    server_id = random.sample(friend.workspace_files, 1)[0]
                else:
                    server_id = friend.file0_id

            start = time.time()
            response = get_content(oauth, server_id, is_ss_provider)
            end = time.time()

            if response.status_code != 200:
                raise ValueError(
                    "Error on response with status_code %d and text %s" % (response.status_code, response.text))
            elapsed = end - start
            if "content-length" not in response.headers:
                size = len(response.content)
                # process_debug_log("Get without response.headers content length status_code %d %s" % (response.status_code, response))
            else:
                size = response.headers["content-length"]

            process_log(str(repr(start)), str(user_id), str(event_args[TraceProcessor.csv_user_type]),
                        str(event_args[TraceProcessor.csv_req_type]),
                        str(TraceProcessor.all_users_dict[user_id].provider),
                        str(TraceProcessor.all_users_dict[friend_id].provider), str(friend_id), str(server_id),
                        str(event_args[TraceProcessor.csv_node_type]), str(size), str(elapsed),
                        str(len(TraceProcessor.all_users_dict[user_id].friends_id_factor_dict)), "NULL")
        except Exception as e:
            error_msg = "Exception at GetContentResponse: trace %s. Error Description: type=%s message={%s} args={%s}" % (
                event_args, type(e), e.message, e.args)
            elapsed = time.time() - start
            process_error_log(error_msg)
            process_log(str(repr(start)), str(user_id), str(event_args[TraceProcessor.csv_user_type]),
                        str(event_args[TraceProcessor.csv_req_type]),
                        str(TraceProcessor.all_users_dict[user_id].provider),
                        str(TraceProcessor.all_users_dict[friend_id].provider), str(friend_id), str(node_id),
                        str(event_args[TraceProcessor.csv_node_type]), "NULL", str(elapsed),
                        str(len(TraceProcessor.all_users_dict[user_id].friends_id_factor_dict)), error_msg)

    def process_delete(self, event_args, ops_counter):
        process_debug_log("Count %d ProcessUnlink node_id %d of user_id %s" % (ops_counter,
                                                                               int(event_args[
                                                                                       TraceProcessor.csv_node_id]),
                                                                               int(event_args[
                                                                                       TraceProcessor.csv_user_id])))

        user_id = int(event_args[TraceProcessor.csv_user_id])
        node_id = int(event_args[TraceProcessor.csv_node_id])
        is_folder = event_args[TraceProcessor.csv_node_type] == "Directory"
        friend_id = int(event_args[TraceProcessor.csv_friend_id])

        friend = TraceProcessor.all_users_dict[friend_id]
        oauth = friend.oauth
        is_ss_provider = friend.provider == "SS"

        fake_delete = False
        start = time.time()
        try:
            server_id = None
            if node_id in friend.node_server_id_dict:
                server_id = friend.node_server_id_dict[node_id]

            if is_folder:
                if server_id not in friend.workspace_folders:
                    if len(friend.workspace_folders) > 0:
                        server_id = random.sample(friend.workspace_folders, 1)[0]
                    else:
                        server_id = friend.shared_folder_id
                        fake_delete = True
            else:
                if server_id not in friend.workspace_files:
                    if len(friend.workspace_files) > 0:
                        server_id = random.sample(friend.workspace_files, 1)[0]
                    else:
                        server_id = friend.file0_id
                        fake_delete = True

            if not fake_delete:
                start = time.time()
                response = unlink(oauth, server_id, is_folder, is_ss_provider)
                end = time.time()

                if response.status_code == 200:
                    if is_folder:
                        friend.workspace_folders.remove(server_id)
                    else:
                        friend.workspace_files.remove(server_id)
                    for k in friend.node_server_id_dict:
                        if friend.node_server_id_dict[k] == server_id:
                            friend.node_server_id_dict.pop(k)
                            break

                    elapsed = end - start

                    process_log(str(repr(start)), str(user_id), str(event_args[TraceProcessor.csv_user_type]),
                                str(event_args[TraceProcessor.csv_req_type]),
                                str(TraceProcessor.all_users_dict[user_id].provider),
                                str(TraceProcessor.all_users_dict[friend_id].provider), str(friend_id),
                                str(server_id),
                                str(event_args[TraceProcessor.csv_node_type]), str(event_args[TraceProcessor.csv_size]),
                                str(elapsed),
                                str(len(TraceProcessor.all_users_dict[user_id].friends_id_factor_dict)), "NULL")
                else:
                    raise ValueError(
                        "Error on response with status_code %d and text %s" % (response.status_code, response.text))
            else:
                process_log(str(repr(time.time())), str(user_id), str(event_args[TraceProcessor.csv_user_type]),
                            str(event_args[TraceProcessor.csv_req_type]),
                            str(TraceProcessor.all_users_dict[user_id].provider),
                            str(TraceProcessor.all_users_dict[friend_id].provider), str(friend_id), "-1",
                            str(event_args[TraceProcessor.csv_node_type]), str(event_args[TraceProcessor.csv_size]),
                            "0",
                            str(len(TraceProcessor.all_users_dict[user_id].friends_id_factor_dict)), "NULL")
        except Exception as e:
            error_msg = "Exception at Unlink: trace %s. Error Description: type=%s message={%s} args={%s}" % (
                event_args, type(e), e.message, e.args)
            process_error_log(error_msg)
            elapsed = time.time() - start
            process_log(str(repr(start)), str(user_id), str(event_args[TraceProcessor.csv_user_type]),
                        str(event_args[TraceProcessor.csv_req_type]),
                        str(TraceProcessor.all_users_dict[user_id].provider),
                        str(TraceProcessor.all_users_dict[friend_id].provider), str(friend_id), str(node_id),
                        str(event_args[TraceProcessor.csv_node_type]), "NULL", str(elapsed),
                        str(len(TraceProcessor.all_users_dict[user_id].friends_id_factor_dict)), error_msg)

    def process_move(self, event_args, ops_counter):
        process_debug_log("Count %d Process MoveResponse node_id %d of user_id %s" % (ops_counter,
                                                                                      int(event_args[
                                                                                              TraceProcessor.csv_node_id]),
                                                                                      int(event_args[
                                                                                              TraceProcessor.csv_user_id])))
        pass
        user_id = int(event_args[TraceProcessor.csv_user_id])
        node_id = int(event_args[TraceProcessor.csv_node_id])
        is_folder = event_args[TraceProcessor.csv_node_type] == "Directory"
        friend_id = int(event_args[TraceProcessor.csv_friend_id])

        user = TraceProcessor.all_users_dict[user_id]
        friend = TraceProcessor.all_users_dict[friend_id]
        oauth = friend.oauth
        is_ss_provider = friend.provider == "SS"
        destination_folder = friend.shared_folder_id
        start = time.time()
        try:
            server_id = None
            if node_id in friend.node_server_id_dict:
                server_id = friend.node_server_id_dict[node_id]

            if is_folder:
                if server_id not in user.workspace_folders:
                    if len(user.workspace_folders) > 0:
                        server_id = random.sample(user.workspace_folders, 1)[0]
                    else:
                        raise ValueError("Error friend %d workspace does not have any folder to move" % (friend_id))
                else:
                    raise ValueError("Error friend %d workspace does not have any folder" % (friend_id))
            else:
                if server_id not in user.workspace_files:
                    if len(user.workspace_files) > 0:
                        server_id = random.sample(user.workspace_files, 1)[0]
                    else:
                        raise ValueError("Error friend %d workspace does not have any file to move" % (friend_id))
                else:
                    raise ValueError("Error friend %d workspace does not have any file" % (friend_id))

            start = time.time()
            response = move(oauth, server_id, destination_folder, is_folder, is_ss_provider)
            end = time.time()

            if response.status_code != 200:
                raise ValueError(
                    "Error on response with status_code %d and text %s" % (response.status_code, response.text))
            elapsed = end - start

            if is_folder:
                user.workspace_folders.remove(server_id)
                friend.workspace_folders.append(server_id)
            else:
                user.workspace_files.remove(server_id)
                friend.workspace_files.append(server_id)

            process_log(str(repr(start)), str(user_id), str(event_args[TraceProcessor.csv_user_type]),
                        str(event_args[TraceProcessor.csv_req_type]),
                        str(TraceProcessor.all_users_dict[user_id].provider),
                        str(TraceProcessor.all_users_dict[friend_id].provider), str(friend_id), str(server_id),
                        str(event_args[TraceProcessor.csv_node_type]), str(event_args[TraceProcessor.csv_size]),
                        str(elapsed),
                        str(len(TraceProcessor.all_users_dict[user_id].friends_id_factor_dict)), "NULL")
        except Exception as e:
            error_msg = "Exception at MoveResponse: trace %s. Error Description: type=%s message={%s} args={%s}" % (
                event_args, type(e), e.message, e.args)
            process_error_log(error_msg)
            elapsed = time.time() - start
            process_log(str(repr(start)), str(user_id), str(event_args[TraceProcessor.csv_user_type]),
                        str(event_args[TraceProcessor.csv_req_type]),
                        str(TraceProcessor.all_users_dict[user_id].provider),
                        str(TraceProcessor.all_users_dict[friend_id].provider), str(friend_id), str(node_id),
                        str(event_args[TraceProcessor.csv_node_type]), "NULL", str(elapsed),
                        str(len(TraceProcessor.all_users_dict[user_id].friends_id_factor_dict)), error_msg)


if __name__ == "__main__":
    print "Error: This class must be instantiated"
