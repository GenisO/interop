# encoding: utf-8
from collections import defaultdict
from API_manager import *
from trace_processor import *
from random import random
from decimal import *

interop_u1_provider = dict()
interop_users_nec_to_u1 = dict()

interop_sharing_factor = defaultdict(lambda: defaultdict(Decimal))


def translate_nec_to_u1(script_path):
    u1_users = list()
    nec_users = list()

    with open(script_path + "/../traces/interop_u1_backup_users_id.csv", "r") as fp:
        for line in fp:
            user_id = str(line)
            u1_users.append(user_id)
            if random() > 0.5:
                interop_u1_provider[user_id] = "NEC"
            else:
                interop_u1_provider[user_id] = "SS"

    with open(script_path + "/../traces/interop_u1_cdn_users_id.csv", "r") as fp:
        for line in fp:
            user_id = str(line)
            u1_users.append(user_id)
            if random() > 0.5:
                interop_u1_provider[user_id] = "NEC"
            else:
                interop_u1_provider[user_id] = "SS"

    with open(script_path + "/../traces/interop_nec_users_id.csv", "r") as fp:
        for line in fp:
            user_id = str(line.split("\n")[0])
            nec_users.append(user_id)

    u1_size = len(u1_users)  # 3473
    nec_size = len(nec_users)  # 3344

    with open(script_path + "/../traces/nec_to_u1.csv", "w") as fp:
        line = "nec_id,u1_id\n"
        fp.write(line)
        for i in range(0, nec_size):
            interop_users_nec_to_u1[nec_users[i]] = str(u1_users[i])
            line = "%s,%s\n" % (nec_users[i], str(u1_users[i]))
            fp.write(line)

    with open(script_path + "/../traces/nec_sharing_user1_user2_files.csv", "r") as fp:
        for i, line in enumerate(fp):
            if i > 0:
                sharing = line.rstrip("\n").split(",")
                user1_id = str(interop_users_nec_to_u1[sharing[0]])
                user2_id = str(interop_users_nec_to_u1[sharing[1]])
                factor = str(Decimal(int(sharing[2])) / Decimal(int(sharing[3])))
                user1_dict = interop_sharing_factor[user1_id]
                user1_dict[user2_id] = factor

    fp = open(script_path + "/../traces/users_sharing_factor.csv", "w")
    fp.write("user1_id,user2_id,sharing_factor\n")
    for user1_id in interop_sharing_factor:
        for user2_id in interop_sharing_factor[user1_id]:
            deci = interop_sharing_factor[user1_id][user2_id]
            stringi = str(deci)
            line = "%s,%s,%s\n" % (user1_id, user2_id, stringi)
            fp.write(line)
    fp.close()

    fss = open(script_path + "/../traces/users_ss_provider.csv", "w")
    fnec = open(script_path + "/../traces/users_nec_provider.csv", "w")
    for user1_id in interop_u1_provider:
        line = "%s\n" % (user1_id)
        if interop_u1_provider[user1_id] == "SS":
            fss.write(line)
        else:
            fnec.write(line)
    fss.close()
    fnec.close()


def retrieve_credentials(users_path, credentials_path, is_ss_provider):
    if is_ss_provider:
        provider = "SS"
        email = "@stacksync.org"
    else:
        provider = "NEC"
        email = "@nec.com"

    # user_id, owner_key, owner_secret, provider
    with open(credentials_path, "w") as fw:
        fw.write("user_id,owner_key,owner_secret\n")
        # user_id
        with open(users_path, "r") as fp:
            for i, line in enumerate(fp):
                if i > 0:
                    user_id = line.rstrip('\n')
                    if user_id != "":
                        user_email = user_id + email
                        [owner_key, owner_secret] = authenticate_request(user_email, user_id, CLIENT_KEY, CLIENT_SECRET,
                                                                         is_ss_provider)
                        sentence = "%s,%s,%s,%s\n" % (user_id, owner_key, owner_secret, provider)
                        fw.write(sentence)


def initialize_scenario(credentials_path, scenario_path):
    # user_id, owner_key, owner_secret, provider, folder0_id, file0_id
    with open(scenario_path, "w") as fw:
        fw.write("user_id,owner_key,owner_secret,provider,folder0_id,file0_id\n")
        # user_id, owner_key, owner_secret, provider
        with open(credentials_path, "r") as fp:
            for i, line in enumerate(fp):
                if i > 0:
                    line = line.rstrip('\n').split(",")
                    user_id = line[0]
                    owner_key = line[1]
                    owner_secret = line[2]
                    provider = line[3]
                    is_ss = provider == "SS"

                    oauth = OAuth1(CLIENT_KEY,
                                   client_secret=CLIENT_SECRET,
                                   resource_owner_key=owner_key,
                                   resource_owner_secret=owner_secret)

                    # TODO: only for testing
                    if not is_ss:
                        parent_id = 9472
                    else:
                        parent_id = 0

                    response = make(oauth, "shared_folder", parent_id=parent_id, is_folder=True, is_ss_provider=is_ss)
                    if response.status_code == 201:
                        json_data = json.loads(response.text)
                        folder_id = str(json_data["id"])
                    elif (
                            response.status_code == 400 or response.status_code == 403) and "Folder already exist" in response.text:
                        response = list_content(oauth, parent_id=parent_id, is_ss_provider=is_ss)
                        json_data = response.json()
                        content_root = json_data["contents"]
                        folder_id = None
                        for tuppla in content_root:
                            try:
                                name = tuppla["filename"]
                                is_folder = tuppla["is_folder"]
                                if name == "shared_folder" and is_folder:
                                    folder_id = tuppla["id"]
                                    break
                            except:
                                raise ValueError("Error with folder initialization")
                        if folder_id is None:
                            raise ValueError("Error with folder initialization")
                    else:
                        raise ValueError("Error with folder initialization")

                    response = make(oauth, "file0.txt", parent_id=folder_id, is_folder=False, is_ss_provider=is_ss)
                    if response.status_code == 201:
                        json_data = json.loads(response.text)
                        file_id = str(json_data["id"])
                    elif (response.status_code == 400 or response.status_code == 403) and \
                            ("This name is already used in the same folder. Please use a different one."
                             in response.text or "File already exist" in response.text):
                        response = list_content(oauth, parent_id=folder_id, is_ss_provider=is_ss)
                        json_data = response.json()
                        content_root = json_data["contents"]
                        file_id = None
                        for tuppla in content_root:
                            try:
                                name = tuppla["filename"]
                                is_folder = tuppla["is_folder"]
                                if name == "file0.txt" and not is_folder:
                                    file_id = tuppla["id"]
                                    break
                            except:
                                raise ValueError("Error with file initialization")
                        if file_id is None:
                            raise ValueError("Error with file initialization")
                    else:
                        raise ValueError("Error with file initialization")

                    sentence = "%s,%s,%s,%s,%s,%s\n" % (user_id, owner_key, owner_secret, provider, folder_id, file_id)
                    fw.write(sentence)


def process_friendship(scenario_path, relations_path, interop_path):
    users_dict = dict()

    # user_id, owner_key, owner_secret, provider, folder0_id, file0_id
    with open(scenario_path, "r") as fp:
        for i, line in enumerate(fp):
            if i > 0:
                array_line = line.rstrip("\n").split(",")
                user_id = str(array_line[0])
                owner_key = str(array_line[1])
                owner_secret = str(array_line[2])
                provider = str(array_line[3])
                folder0_id = str(array_line[4])
                file0_id = str(array_line[5])

                oauth = OAuth1(CLIENT_KEY,
                               client_secret=CLIENT_SECRET,
                               resource_owner_key=owner_key,
                               resource_owner_secret=owner_secret)

                # def User(user_id, oauth, shared_folder_id, provider, =dict(), file0_id=None):
                user = User(user_id, oauth, folder0_id, provider, file0_id=file0_id)
                users_dict[user_id] = user

    # user1_id, user2_id, sharing_factor
    with open(relations_path, "r") as fp:
        for i, line in enumerate(fp):
            if i > 0:
                try:
                    array_line = line.rstrip("\n").split(",")
                    user1_id = str(array_line[0])
                    user2_id = str(array_line[1])
                    factor = str(array_line[2])

                    if user1_id in users_dict and user2_id in users_dict:
                        # user2_id share his folder with user1
                        user1 = users_dict[user1_id]
                        user2 = users_dict[user2_id]
                        user2_oauth = user2.oauth
                        user2_shared_folder_id = users_dict[user2_id].shared_folder_id
                        if user1_id == user2_id:
                            user1.friends_id_factor_dict[user2_id] = factor
                        else:
                            # TODO: Add nec provider
                            if user1.provider == "SS":
                                friend_email = "%s@stacksync.org" % (user1.id)
                            else:
                                friend_email = "%s@nec.com" % (user1.id)
                            response = share(user2_oauth, user2_shared_folder_id, friend_email)
                            if response.status_code == 201:
                                user1.friends_id_factor_dict[user2_id] = factor
                except KeyError:
                    pass

    # user_id, owner_key, owner_secret, provider, shared_folder_id, file0_id, friends_num, user2, factor2, user3, factor3, ...
    with open(interop_path, "w") as fp:
        fp.write(
            "user_id,owner_key,owner_secret,provider,shared_folder_id,file0_id,friends_num,user2,factor2,user3,factor3,...\n")
        for user_id in users_dict:
            user = users_dict[user_id]
            line = "%s,%s,%s,%s,%s,%s,%s" % (
                str(user.id), user.oauth.client.resource_owner_key, user.oauth.client.resource_owner_secret,
                user.provider, str(user.shared_folder_id), str(user.file0_id),
                str(len(user.friends_id_factor_dict.keys())))
            for friend_id in user.friends_id_factor_dict:
                line += ",%s,%s" % (friend_id, user.friends_id_factor_dict[friend_id])
            line += "\n"
            fp.write(line)


def users_table(nec_path, ss_path, relations_path, table_path):
    nec_users = list()
    ss_users = list()

    relation_dict = defaultdict(list)

    # user_id
    with open(nec_path, "r") as fp:
        for i, line in enumerate(fp):
            if i > 0:
                user_id = line.rstrip("\n")
                nec_users.append(user_id)

    # user_id
    with open(ss_path, "r") as fp:
        for i, line in enumerate(fp):
            if i > 0:
                user_id = line.rstrip("\n")
                ss_users.append(user_id)


    # user1_id, user2_id, sharing_factor
    with open(relations_path, "r") as fp:
        for i, line in enumerate(fp):
            if i > 0:
                array_line = line.rstrip("\n").split(",")
                user1_id = str(array_line[0])
                user2_id = str(array_line[1])

                if user1_id in ss_users:
                    friend_email = "%s@stacksync.org" % (user1_id)
                else:
                    friend_email = "%s@nec.com" % (user1_id)
                if friend_email not in relation_dict[user2_id]:
                    relation_dict[user2_id].append(friend_email)


    # user_email, user_pass, friends_list
    with open(table_path + ".nec", "w") as fnec:
        fnec.write("user_email, user_pass, friends_list\n")
        with open(table_path + ".ss", "w") as fss:
            fss.write("user_email, user_pass, friends_list\n")

            for user_id in relation_dict:
                friends_list = ""
                for friend_email in relation_dict[user_id]:
                    friends_list += ", %s" % (friend_email)

                if user_id in ss_users:
                    line = "%s@stacksync.org, %s%s\n" % (user_id, user_id, friends_list)
                    fss.write(line)
                else:
                    line = "%s@nec.com, %s%s\n" % (user_id, user_id, friends_list)
                    fnec.write(line)


if __name__ == "__main__":
    script_path = os.path.realpath(__file__)[:os.path.realpath(__file__).rfind("/")]

    # Fixed
    relations_path = script_path + "/../target/users_sharing_factor.csv"

    # Editable
    # StackSync
    ast3_users_path = script_path + "/../target/ast3_users.csv"
    ast3_credentials_path = script_path + "/../target/ast3_users_credentials.csv"
    ast3_server_id_path = script_path + "/../target/ast3_users_credentials_server_id.csv"
    # final_path = script_path + "/../target/ast3_full_interop_info.csv"
    # NEC
    nec_users_path = script_path + "/../target/nec_users.csv"
    nec_credentials_path = script_path + "/../target/nec_users_credentials.csv"
    nec_server_id_path = script_path + "/../target/nec_users_credentials_server_id.csv"
    final_path = script_path + "/../target/nec_full_interop_info.csv"

    nec_path = script_path + "/../target/users_nec_provider.csv"
    ss_path = script_path + "/../target/users_ss_provider.csv"
    table_path = script_path + "/../target/interop_list_users_with_friends.csv"
    try:
        # translate_nec_to_u1(script_path)
        # load_interop_users(script_path)

        # users_table(nec_path, ss_path, relations_path, table_path)

        # Correct steps
        # retrieve_credentials(nec_users_path, nec_credentials_path, False)
        # retrieve_credentials(ast3_users_path, ast3_credentials_path, True)
        # credentials_path = script_path + "/../target/mini_test_users_credentials.csv"
        initialize_scenario(ast3_credentials_path, ast3_server_id_path)
        # process_friendship(data_path, relations_path, final_path)
    except (KeyboardInterrupt, SystemExit):
        print ("\nExperiment killed")
