# encoding: utf-8
from collections import defaultdict

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
            user_id = int(line)
            u1_users.append(user_id)
            if random() > 0.5:
                interop_u1_provider[user_id] = "NEC"
            else:
                interop_u1_provider[user_id] = "SS"

    with open(script_path + "/../traces/interop_u1_cdn_users_id.csv", "r") as fp:
        for line in fp:
            user_id = int(line)
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
            interop_users_nec_to_u1[nec_users[i]] = int(u1_users[i])
            line = "%s,%d\n" % (nec_users[i], int(u1_users[i]))
            fp.write(line)


    with open(script_path + "/../traces/nec_sharing_user1_user2_files.csv", "r") as fp:
        for i, line in enumerate(fp):
            if i > 0:
                sharing = line.split(',')
                user1_id = int(interop_users_nec_to_u1[sharing[0]])
                user2_id = int(interop_users_nec_to_u1[sharing[1]])
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
        line = "%d\n" % (user1_id)
        if interop_u1_provider[user1_id] == "SS":
            fss.write(line)
        else:
            fnec.write(line)
    fss.close()
    fnec.close()


def generate_interop_relations(script_path):
    CLIENT_KEY = "b3af4e669daf880fb16563e6f36051b105188d413"
    CLIENT_SECRET = "c168e65c18d75b35d8999b534a3776cf"

    users_dict = dict()

    #user_id,owner_key,owner_secret,server_id,provider
    with open(script_path + "/../traces/credentials_ss_provider.csv", "r") as fp:
        for i, line in enumerate(fp):
            if i > 1:
                array_line = line.rstrip('\n').split(",")
                user_id = int(array_line[0])
                owner_key = str(array_line[1])
                owner_secret = str(array_line[2])
                shared_folder_id = str(array_line[3])
                provider = str(array_line[4])

                oauth = OAuth1(CLIENT_KEY,
                   client_secret=CLIENT_SECRET,
                   resource_owner_key=owner_key,
                   resource_owner_secret=owner_secret)

                #def User(self,user_id, oauth, shared_folder_id, provider):
                user = User(user_id, oauth, shared_folder_id, provider)
                users_dict[user_id] = user

    #user1_id,user2_id,sharing_factor
    with open(script_path + "/../traces/users_sharing_factor.csv", "r") as fp:
        for i, line in enumerate(fp):
            if i > 1:
                try:
                    array_line = line.rstrip('\n').split(",")
                    user1_id = int(array_line[0])
                    user2_id = int(array_line[1])
                    factor = str(array_line[2])

                    #user2_id share his folder with user1
                    user1 = users_dict[user1_id]
                    user2 = users_dict[user2_id]
                    user2_oauth = user2.oauth
                    user2_shared_folder_id = users_dict[user2_id].shared_folder_id
                    # TODO: Add nec provider
                    if user1.provider == "ss":
                        friend_email = "%d@stacksync.org" % (user1.id)
                    else:
                        friend_email = "%d@nec.org" % (user1.id)
                    response = share(user2_oauth, user2_shared_folder_id, friend_email)
                    if response.status_code == 201:
                        user1.friends_dict[user2] = factor
                except KeyError:
                    pass
    #user_id,owner_key,owner_secret,shared_folder_id,provider,friends_num,user2,factor2,user3,factor3,...
    with open(script_path + "/../traces/interop_table.csv", "w") as fp:
        fp.write("user_id,owner_key,owner_secret,shared_folder_id,provider,friends_num,user2,factor2,user3,factor3,...\n")
        for user_id in users_dict:
            user = users_dict[user_id]
            line = "%d,%s,%s,%s,%s,%d" % (user.id, user.oauth.client.resource_owner_key, user.oauth.client.resource_owner_secret, user.shared_folder_id, user.provider, len(user.friends_dict.keys()))
            for friend in user.friends_dict:
                line += ",%d,%s" % (friend.id, user.friends_dict[friend])
            line += "\n"
            fp.write(line)


if __name__ == "__main__":
    script_path = os.path.realpath(__file__)[:os.path.realpath(__file__).rfind("/")]
    try:
        translate_nec_to_u1(script_path)
        # load_interop_users(script_path)
    except (KeyboardInterrupt, SystemExit):
        print ("\nExperiment killed")

