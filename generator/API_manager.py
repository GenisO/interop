# encoding: utf-8
from __future__ import unicode_literals
import sys, os
import unittest
import urllib2

import requests
import json
import urllib
from urlparse import parse_qs
import urlparse
from requests_oauthlib import OAuth1, OAuth1Session

CLIENT_KEY = "b3af4e669daf880fb16563e6f36051b105188d413"
CLIENT_SECRET = "c168e65c18d75b35d8999b534a3776cf"

# TODO: Take parameters from a config file
ip_ss = "localhost"
ip_nec = "localhost"

URL_STACKSYNC = 'http://%s/v1' % (ip_ss)
URL_NEC = 'http://%s' % (ip_nec)


def put_content(oauth, file_id, file_path, is_ss_provider):
    headers = {}
    if is_ss_provider:
        URL_BASIC = URL_STACKSYNC
    else:
        URL_BASIC = URL_NEC

    url = URL_BASIC + '/file/' + str(file_id) + '/data'
    headers['StackSync-API'] = "v2"
    headers['Content-Type'] = "text/plain"
    with open(file_path, "r") as myfile:
        data = myfile.read()
    r = requests.put(url, data=data, headers=headers, auth=oauth)
    return r


def get_content(oauth, file_id, is_ss_provider=True):
    headers = {}
    if is_ss_provider:
        URL_BASIC = URL_STACKSYNC
    else:
        URL_BASIC = URL_NEC

    url = URL_BASIC + '/file/' + str(file_id) + '/data'

    headers['StackSync-API'] = "v2"
    headers['Content-Type'] = "application/json"
    r = requests.get(url, headers=headers, auth=oauth)
    return r


def list_content(oauth, parent_id=0, is_ss_provider=True):
    headers = {}
    if is_ss_provider:
        URL_BASIC = URL_STACKSYNC
    else:
        URL_BASIC = URL_NEC

    url = URL_BASIC + '/folder/' + str(parent_id) + "/contents"

    headers['StackSync-API'] = "v2"
    headers['Content-Type'] = "application/json"
    r = requests.get(url, headers=headers, auth=oauth)
    return r


def make(oauth, name, parent_id=0, is_folder=False, is_ss_provider=True):
    headers = {}
    headers['StackSync-API'] = "v2"
    headers['Content-Type'] = "application/json"

    if is_ss_provider:
        URL_BASIC = URL_STACKSYNC
        data = None
    else:
        URL_BASIC = URL_NEC
        data = ""

    if not name:
        raise ValueError("Can not create a folder without name")
    if is_folder:
        url = URL_BASIC + '/folder'
        if parent_id == 0:
            parameters = {"name": str(name)}
        else:
            parameters = {"name": str(name), "parent": parent_id}
        r = requests.post(url, json.dumps(parameters), headers=headers, auth=oauth)
        return r
    else:
        if parent_id == 0:
            url = URL_BASIC + '/file?name=' + str(name)
        else:
            url = URL_BASIC + '/file?name=' + str(name) + "&parent=" + str(parent_id)
        r = requests.post(url, data=data, headers=headers, auth=oauth)
        return r


def unlink(oauth, item_id, is_folder=False, is_ss_provider=True):
    headers = {}
    if is_ss_provider:
        URL_BASIC = URL_STACKSYNC
    else:
        URL_BASIC = URL_NEC

    if is_folder:
        url = URL_BASIC + '/folder/' + str(item_id)
    else:
        url = URL_BASIC + '/file/' + str(item_id)

    headers['StackSync-API'] = "v2"
    headers['Content-Type'] = "text/plain"
    r = requests.delete(url, headers=headers, auth=oauth)
    return r


# TODO: Problems with move over two distinct providers?
def move(oauth, item_id, new_parent=0, is_folder=False, is_ss_provider=True):
    headers = {}
    if is_ss_provider:
        URL_BASIC = URL_STACKSYNC
    else:
        URL_BASIC = URL_NEC

    if is_folder:
        url = URL_BASIC + '/folder/' + str(item_id)
    else:
        url = URL_BASIC + '/file/' + str(item_id)

    parameters = {"parent": str(new_parent)}

    headers['StackSync-API'] = "v2"
    headers['Content-Type'] = "application/json"
    r = requests.put(url, json.dumps(parameters), headers=headers, auth=oauth)
    return r


def authenticate_request(useremail, password, client_key, client_secret, is_ss_provider=True):
    if is_ss_provider:
        REQUEST_TOKEN_ENDPOINT = "http://%s/oauth/request_token" % (ip_ss)
        ACCESS_TOKEN_ENDPOINT = "http://%s/oauth/access_token" % (ip_ss)
        AUTHORIZE_ENDPOINT = "http://%s/oauth/authorize" % (ip_ss)
    else:
        REQUEST_TOKEN_ENDPOINT = "http://%s/api/cloudspaces/oauth/request_token" % (ip_nec)
        ACCESS_TOKEN_ENDPOINT = "http://%s/api/cloudspaces/oauth/access_token" % (ip_nec)
        AUTHORIZE_ENDPOINT = "http://%s/api/cloudspaces/oauth/authorize" % (ip_nec)

    oauth = OAuth1(client_key=client_key, client_secret=client_secret, callback_uri='oob')
    headers = {"STACKSYNC_API": "v2"}
    try:
        r = requests.post(url=REQUEST_TOKEN_ENDPOINT, auth=oauth, headers=headers, verify=False)
        if r.status_code != 200:
            raise ValueError("Error in the authenticate process")
    except:
        raise ValueError("Error in the authenticate process")

    credentials = parse_qs(r.content)
    resource_owner_key = credentials.get('oauth_token')[0]
    resource_owner_secret = credentials.get('oauth_token_secret')[0]

    authorize_url = AUTHORIZE_ENDPOINT + '?oauth_token=' + resource_owner_key

    params = urllib.urlencode({'email': useremail, 'password': password, 'permission': 'allow'})
    headers = {"Content-Type": "application/x-www-form-urlencoded", "STACKSYNC_API": "v2"}
    try:
        response = requests.post(authorize_url, data=params, headers=headers, verify=False)
    except:
        raise ValueError("Error in the authenticate process 1")
    if "application/x-www-form-urlencoded" == response.headers['Content-Type']:
        parameters = parse_qs(response.content)
        verifier = parameters.get('verifier')[0]

        oauth2 = OAuth1(client_key,
                        client_secret=client_secret,
                        resource_owner_key=resource_owner_key,
                        resource_owner_secret=resource_owner_secret,
                        verifier=verifier,
                        callback_uri='oob')
        try:
            r = requests.post(url=ACCESS_TOKEN_ENDPOINT, auth=oauth2, headers=headers, verify=False)
        except:
            raise ValueError("Error in the authenticate process 2")
        credentials = parse_qs(r.content)
        resource_owner_key = credentials.get('oauth_token')[0]
        resource_owner_secret = credentials.get('oauth_token_secret')[0]

        return resource_owner_key, resource_owner_secret

    raise ValueError("Error in the authenticate process 3")


def share(oauth, folder_id, friend_mail):
    headers = {}
    url = URL_STACKSYNC + "/folder/" + str(folder_id) + "/share"

    headers["StackSync-API"] = "v2"
    headers["Content-Type"] = "application/json"

    shared_to = friend_mail.split(",")
    r = requests.post(url, json.dumps(shared_to), headers=headers, auth=oauth)
    # return 201 if tot ok, nothing to parse of response
    return r


if __name__ == "__main__":
    argv_list = sys.argv
    if argv_list[1] == "auth":
        print authenticate_request(argv_list[2], argv_list[3], argv_list[4], argv_list[5])
    elif argv_list[1] == "make_shared":
        oauth = OAuth1(argv_list[4],
                       client_secret=argv_list[5],
                       resource_owner_key=argv_list[2],
                       resource_owner_secret=argv_list[3])
        folder_name = "shared"
        response = make(oauth, folder_name, is_folder=True)
        if response.status_code == 201:
            json_data = json.loads(response.text)
            server_id = int(json_data["id"])
        elif response.status_code == 400 and (
                        "This name is already used in the same folder. Please use a different one." in response.text or
                        "Folder already exists." in response.text):
            response = list_content(oauth)
            json_data = response.json()
            content_root = json_data["contents"]
            for line in content_root:
                try:
                    name = line["filename"]
                    is_folder = line["is_folder"]
                    if name == folder_name and is_folder:
                        server_id = line["id"]
                        break
                except KeyError:
                    pass
        print server_id
    elif argv_list[1] == "share":
        oauth = OAuth1(argv_list[6],
                       client_secret=argv_list[7],
                       resource_owner_key=argv_list[4],
                       resource_owner_secret=argv_list[5])
        response = share(oauth, argv_list[2], argv_list[3])
        print response.status_code, response.text
