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

# TODO: Take parameters from a config file
URL_STACKSYNC = 'https://IP:8080/v1'
STACKSYNC_REQUEST_TOKEN_ENDPOINT = "http://IP:8080/oauth/request_token"
STACKSYNC_ACCESS_TOKEN_ENDPOINT = "http://IP:8080/oauth/access_token"
STACKSYNC_AUTHORIZE_ENDPOINT = "http://IP:8080/oauth/authorize"

# TODO: convert to update file
def put_content(oauth, file_name, file_path,  parent=0):
    print "PUT"
    headers = {}
    if name:
        if parent!=0:
    	    url = URL_STACKSYNC +"/file?name="+name+"&parent="+parent
    	else:
    	    url = URL_STACKSYNC +"/file?name="+name
        with open (file_path, "r") as myfile:
            data=myfile.read()
        headers['StackSync-API'] = "v2"
        r = requests.post(url, data=data, headers=headers, auth=oauth)
        return r
    else:
    	raise ValueError('Bad parameters')

def get_content(oauth, file_id):
    headers = {}
    url = URL_STACKSYNC +'/file/'+str(file_id)+'/data'
    # uri, headers, _ = client.sign(url, http_method='GET')
    headers['StackSync-API'] = "v2"
    headers['Content-Type'] = "application/json"
    r = requests.get(url, headers=headers, auth=oauth)
    return r

# TODO: add distinction between folder and file, parent always root
def make(oauth, name, parent=None):
    headers = {}
    url = BASE_URL +'/folder'
    # uri, headers, _ = client.sign(url, http_method='GET')
    if not name:
        raise ValueError("Can not create a folder without name")
    else:
        if not parent:
    	    parameters = {"name":str(name)}
        else:
            parameters = {"name":str(new_name), "parent":new_parent}
        headers['StackSync-API'] = "v2"
        headers['Content-Type'] = "application/json"
        r = requests.post(url, json.dumps(parameters), headers=headers, auth=oauth)
        return r

def unlink(oauth, item_id, is_folder=False):
    #TODO: Implement the method for folders and files.
    headers = {}
    if is_folder
        url = URL_STACKSYNC +'/folder/'+str(item_id)
    else:
        url = URL_STACKSYNC +'/file/'+str(item_id)
    # uri, headers, _ = client.sign(url, http_method='GET')
    headers['StackSync-API'] = "v2"
    headers['Content-Type'] = "text/plain"
    r = requests.delete(url, headers=headers, auth=oauth)
    return r

def authenticate_request(username, password, client_key, client_secret):
    username = request.POST['username']
    password = request.POST['password']
    oauth = OAuth1(client_key=client_key, client_secret=client_secret, callback_uri='oob')
    headers = {"STACKSYNC_API":"v2"}
    try:
        r = requests.post(url=STACKSYNC_REQUEST_TOKEN_ENDPOINT, auth=oauth, headers=headers, verify=False)
        if r.status_code != 200:
            raise ValueError("Error in the authenticate process")
    except:
        raise ValueError("Error in the authenticate process")

    credentials = parse_qs(r.content)
    resource_owner_key = credentials.get('oauth_token')[0]
    resource_owner_secret = credentials.get('oauth_token_secret')[0]

    authorize_url = STACKSYNC_AUTHORIZE_ENDPOINT + '?oauth_token=' + resource_owner_key

    params = urlencode({'email': username, 'password': password, 'permission':'allow'})
    headers = {"Content-Type":"application/x-www-form-urlencoded", "STACKSYNC_API":"v2"}
    try:
        response = requests.post(authorize_url, data=params, headers=headers, verify=False)
    except:
        raise ValueError("Error in the authenticate process")

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
            r = requests.post(url=settings.STACKSYNC_ACCESS_TOKEN_ENDPOINT, auth=oauth2, headers=headers, verify=False)
        except:
            raise ValueError("Error in the authenticate process")
        credentials = parse_qs(r.content)
        resource_owner_key = credentials.get('oauth_token')[0]
        resource_owner_secret = credentials.get('oauth_token_secret')[0]

        return resource_owner_key, resource_owner_secret

    raise ValueError("Error in the authenticate process")

# TODO: Implement
def move(oauth, item_id, is_folder=False):
