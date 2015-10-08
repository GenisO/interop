# encoding: utf-8
import time
import sys
import random
import json
import os
import collections
import subprocess
import threading
import logging
import logging.handlers
import copy

from API_manager import *
from requests_oauthlib import OAuth1

log_file_trace_path = __file__[:__file__.rfind("/")] + "/../interop_experiment.log"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(log_file_trace_path, maxBytes=20000000, backupCount=10)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("[%(levelname)s];%(asctime)s;%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def process_log(tstamp, user_id, req_t, elapsed, node_id, node_type, size):
    logger.info("[TRACE];%s;%s;%s;%s;%s;%s;%s" % (tstamp, user_id, req_t, elapsed, node_id, node_type, size))


def process_debug_log(message):
    logger.info("[DEBUG] - %s" % (message))


def process_error_log(message):
    logger.error(message)


class thread_trace_processor(threading.Thread):
    def __init__(self, p_user_oauth, p_thread_num, p_total_threads, p_trace_path):
        threading.Thread.__init__(self)

        self.node_server_id_dict = dict()
        self.server_folder_dict = collections.defaultdict(list)
        self.server_file_dict = collections.defaultdict(list)

        self.csv_timestamp = 0
        self.csv_normalized_timestamp = 1
        self.csv_user_id = 2
        self.csv_req_type = 3
        self.csv_node_id = 4
        self.csv_node_type = 5
        self.csv_size = 6
        self.csv_user_type = 7

        self.user_oauth = p_user_oauth
        self.thread_id = p_thread_num
        self.num_threads = p_total_threads

        self.trace_path = p_trace_path

    def run(self):
        self.load_initial_environment()
        self.event_dispatcher()

    def load_initial_environment(self):
        for user_id in self.user_oauth:
            if int(user_id) % self.num_threads == self.thread_id:
                fake_event_args = list()
                fake_event_args.append("0")  # csv_timestamp
                fake_event_args.append("0")  # csv_normalized_timestamp
                fake_event_args.append(str(user_id))  # csv_user_id
                fake_event_args.append("MakeResponse")  # csv_req_type
                fake_event_args.append(str(user_id))  # csv_node_id
                fake_event_args.append("File")  # csv_node_type
                fake_event_args.append("0")  # csv_size
                fake_event_args.append("Fake")  # csv_user_type
                self.process_make(fake_event_args)

    def event_dispatcher(self):
        previous_normalized_timestamp = 0
        with open(self.trace_path, "r") as fp:
            for line in fp:
                event = line.split(',')
                t_sleep = int(event[self.csv_normalized_timestamp]) - previous_normalized_timestamp
                time.sleep(t_sleep)
                previous_normalized_timestamp = int(event[self.csv_normalized_timestamp])
                if int(event[self.csv_user_id]) % self.num_threads == self.thread_id:
                    # Process op
                    switcher = {
                        "GetContentResponse": self.process_get,
                        "MakeResponse": self.process_make,
                        "MoveResponse": self.process_move,
                        "PutContentResponse": self.process_put,
                        "Unlink": self.process_delete,
                    }
                    # Get the function from switcher dictionary
                    func = switcher.get(event[self.csv_req_type])
                    func(event)
        process_debug_log("Finished")

    def oauth(self, user_id):
        if int(user_id) not in self.user_oauth:
            raise ValueError("Error no oauth for user %s" % (user_id))
        return self.user_oauth[int(user_id)]

    def process_make(self, event_args):
        process_debug_log("Process MakeResponse node_id %d of user_id %d" % (
            int(event_args[self.csv_node_id]), int(event_args[self.csv_user_id])))
        user_id = int(event_args[self.csv_user_id])
        node_id = int(event_args[self.csv_node_id])
        is_folder = event_args[self.csv_node_type] == "Directory"
        try:
            start = int(time.time())
            response = make(self.oauth(user_id), node_id, is_folder)
            end = int(time.time())
            if response.status_code == 201:
                json_data = json.loads(response.text)
                server_id = int(json_data["id"])
                if node_id not in self.node_server_id_dict:
                    self.node_server_id_dict[node_id] = server_id
                if is_folder and server_id not in self.server_folder_dict[user_id]:
                    self.server_folder_dict[user_id].append(server_id)
                elif not is_folder and server_id not in self.server_file_dict[user_id]:
                    self.server_file_dict[user_id].append(server_id)
                elapsed = int(end - start)
                process_log(str(start), str(user_id), event_args[self.csv_req_type], str(elapsed), str(node_id),
                            event_args[self.csv_node_type], event_args[self.csv_user_type])
            elif response.status_code == 400 and "This name is already used in the same folder. Please use a different one." in response.text:
                # Ensure we have it mapped
                response = list_root_content(self.oauth(user_id))
                json_data = response.json()
                content_root = json_data["contents"]
                for line in content_root:
                    try:
                        name = line["filename"]
                        if name == str(node_id):
                            server_id = line["id"]
                            if node_id not in self.node_server_id_dict:
                                self.node_server_id_dict[node_id] = server_id
                            if is_folder and server_id not in self.server_folder_dict[user_id]:
                                self.server_folder_dict[user_id].append(server_id)
                            elif not is_folder and server_id not in self.server_file_dict[user_id]:
                                self.server_file_dict[user_id].append(server_id)
                            break
                    except KeyError as e:
                        pass
            else:
                raise ValueError(
                    "Error on response with status_code %d and text {%s}" % (response.status_code, response.text))
        except Exception as e:
            process_error_log(
                "Exception at MakeResponse: trace %s. Error Description: type=%s message={%s} args={%s}" % (
                    event_args, type(e), e.message, e.args))

    def process_put(self, event_args):
        process_debug_log("Process PutContentResponse node_id %d of user_id %d" % (
            int(event_args[self.csv_node_id]), int(event_args[self.csv_user_id])))
        user_id = int(event_args[self.csv_user_id])
        node_id = int(event_args[self.csv_node_id])
        size = int(event_args[self.csv_size])
        local_path = "./%s.file" % (self.thread_id)
        try:
            if node_id not in self.node_server_id_dict:
                mod_event_args = copy.deepcopy(event_args)
                mod_event_args[self.csv_node_type] = "File"
                mod_event_args[self.csv_req_type] = "MakeResponse"
                self.process_make(mod_event_args)
            server_id = self.node_server_id_dict[node_id]
            if server_id not in self.server_file_dict[user_id]:
                if len(self.server_file_dict[user_id]) > 0:
                    server_id = random.sample(self.server_file_dict[user_id], 1)[0]
                else:
                    raise ValueError("Error user %s does not have any file to update" % (user_id))
            # TODO: only for testing
            size = int(size / 10.0)
            if size < 1:
                size = 2
            with open(local_path, "w") as f:
                subprocess.check_call(["fallocate", "-l", str(size), local_path])
            start = int(time.time())
            response = put_content(self.oauth(user_id), server_id, local_path)
            end = int(time.time())
            if response.status_code == 200 or response.status_code == 201:
                if server_id not in self.server_file_dict[user_id]:
                    self.server_file_dict[user_id].append(server_id)
                elapsed = int(end - start)
                json_data = json.loads(response.text)
                size = json_data["size"]
                process_log(str(start), str(user_id), event_args[self.csv_req_type], str(elapsed), str(node_id),
                            event_args[self.csv_node_type], size)
            else:
                raise ValueError(
                    "Error on response with status_code %d and text %s" % (response.status_code, response.text))
        except subprocess.CalledProcessError as e:
            process_error_log(
                "Exception at fallocate with size %d: trace %s. Error Description: type=%s message={%s} args={%s}" % (
                    size, event_args, type(e), e.message, e.args))
        except Exception as e:
            process_error_log(
                "Exception at PutContentResponse: trace %s. Error Description: type=%s message={%s} args={%s}" % (
                    event_args, type(e), e.message, e.args))
        finally:
            try:
                os.remove(local_path)
            except:
                pass

    def process_get(self, event_args):
        process_debug_log("Process GetContentResponse node_id %d of user_id %d" % (
            int(event_args[self.csv_node_id]), int(event_args[self.csv_user_id])))
        user_id = int(event_args[self.csv_user_id])
        node_id = int(event_args[self.csv_node_id])
        try:
            if user_id in self.server_file_dict:
                if node_id in self.node_server_id_dict:
                    server_id = self.node_server_id_dict[node_id]
                elif len(self.server_file_dict[user_id]) > 0:
                    server_id = random.sample(self.server_file_dict[user_id], 1)[0]
                else:
                    raise ValueError("Error user %d does not have any file to download" % (user_id))
                start = int(time.time())
                response = get_content(self.oauth(user_id), server_id)
                end = int(time.time())
                if response.status_code != 200:
                    raise ValueError(
                        "Error on response with status_code %d and text %s" % (response.status_code, response.text))
                elapsed = int(end - start)
                size = response.headers["content-length"]
                process_log(str(start), str(user_id), event_args[self.csv_req_type], str(elapsed), str(node_id),
                            event_args[self.csv_node_type], size)
            else:
                raise ValueError("Error user %s does not uploaded any file" % (user_id))
        except Exception as e:
            process_error_log(
                "Exception at GetContentResponse: trace %s. Error Description: type=%s message={%s} args={%s}" % (
                    event_args, type(e), e.message, e.args))

    def process_delete(self, event_args):
        process_debug_log("Process Unlink node_id %d of user_id %d" % (
            int(event_args[self.csv_node_id]), int(event_args[self.csv_user_id])))
        user_id = int(event_args[self.csv_user_id])
        node_id = int(event_args[self.csv_node_id])
        is_folder = event_args[self.csv_node_type] == "Directory"
        fake_delete = False
        try:
            if is_folder:
                if user_id in self.server_folder_dict:
                    if node_id in self.node_server_id_dict:
                        server_id = self.node_server_id_dict[node_id]
                    elif len(self.server_folder_dict[user_id]) > 1:
                        server_id = random.sample(self.server_folder_dict[user_id], 1)[0]
                    elif len(self.server_folder_dict[user_id]) == 1:
                        fake_delete = True
                    else:
                        raise ValueError("Error user %s does not have any folder to delete" % (user_id))
                else:
                    raise ValueError("Error user %s does not uploaded any folder" % (user_id))
            else:
                if user_id in self.server_file_dict:
                    if node_id in self.node_server_id_dict:
                        server_id = self.node_server_id_dict[node_id]
                    elif len(self.server_file_dict[user_id]) > 1:
                        server_id = random.sample(self.server_file_dict[user_id], 1)[0]
                    elif len(self.server_file_dict[user_id]) == 1:
                        fake_delete = True
                    else:
                        raise ValueError("Error user %s does not have any file to delete" % (user_id))
                else:
                    raise ValueError("Error user %s does not uploaded any file" % (user_id))
            if not fake_delete:
                start = int(time.time())
                response = unlink(self.oauth(user_id), server_id, is_folder)
                end = int(time.time())
                if response.status_code == 200:
                    if is_folder:
                        self.server_folder_dict[user_id].remove(server_id)
                    else:
                        self.server_file_dict[user_id].remove(server_id)
                    elapsed = int(end - start)
                    process_log(str(start), str(user_id), event_args[self.csv_req_type], str(elapsed), str(node_id),
                                event_args[self.csv_node_type], event_args[self.csv_node_type])
                else:
                    raise ValueError(
                        "Error on response with status_code %d and text %s" % (response.status_code, response.text))
            else:
                process_log(str(int(time.time())), str(user_id), event_args[self.csv_req_type], "0", str(node_id),
                            event_args[self.csv_node_type], "FAKE")
        except Exception as e:
            process_error_log("Exception at Unlink: trace %s. Error Description: type=%s message={%s} args={%s}" % (
                event_args, type(e), e.message, e.args))

    def process_move(self, event_args):
        process_debug_log("Process MoveResponse node_id %d of user_id %d" % (
            int(event_args[self.csv_node_id]), int(event_args[self.csv_user_id])))
        user_id = int(event_args[self.csv_user_id])
        node_id = int(event_args[self.csv_node_id])
        is_folder = event_args[self.csv_node_type] == "Directory"
        try:
            if is_folder:
                if user_id in self.server_folder_dict:
                    if node_id in self.node_server_id_dict:
                        server_id = self.node_server_id_dict[node_id]
                    elif len(self.server_folder_dict[user_id]) > 0:
                        server_id = random.sample(self.server_folder_dict[user_id], 1)[0]
                    else:
                        raise ValueError("Error user %s does not have any folder to move" % (user_id))
                else:
                    raise ValueError("Error user %s does not uploaded any folder" % (user_id))
            else:
                if user_id in self.server_file_dict:
                    if node_id in self.node_server_id_dict:
                        server_id = self.node_server_id_dict[node_id]
                    elif len(self.server_file_dict[user_id]) > 0:
                        server_id = random.sample(self.server_file_dict[user_id], 1)[0]
                    else:
                        raise ValueError("Error user %s does not have any file to move" % (user_id))
                else:
                    raise ValueError("Error user %s does not uploaded any file" % (user_id))
            start = int(time.time())
            response = move(self.oauth(user_id), server_id, is_folder)
            end = int(time.time())
            if response.status_code != 200:
                raise ValueError(
                    "Error on response with status_code %d and text %s" % (response.status_code, response.text))
            elapsed = int(end - start)
            process_log(str(start), str(user_id), event_args[self.csv_req_type], str(elapsed), str(node_id),
                        event_args[self.csv_node_type], "NULL")
        except Exception as e:
            process_error_log(
                "Exception at MoveResponse: trace %s. Error Description: type=%s message={%s} args={%s}" % (
                    event_args, type(e), e.message, e.args))


if __name__ == "__main__":
    print "Error: This class must be instantiated"
