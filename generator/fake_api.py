# encoding: utf-8
from requests_oauthlib import OAuth1
import logging

logging.basicConfig(filename='example.log',
                level=logging.DEBUG,
                format='[%(levelname)s] (%(threadName)-10s) %(message)s',
                )
def put_content(oauth, file_id, file_path):
    logging.debug( "PUT %s %s %s" %(oauth, file_id, file_path))
    return 200

def get_content(oauth, file_id):
    logging.debug( "GET %s %s" %(oauth, file_id))
    return 200

def make(oauth, name, is_folder=False):
    logging.debug( "MAKE %s %s" %(oauth, name))
    return 201

def unlink(oauth, item_id, is_folder=False):
    logging.debug( "DELETE %s %s" %(oauth, item_id))
    return 200

def authenticate_request(username, password, client_key, client_secret):
    logging.debug( "AUTHENTICATE %s %s %s %s" %(username, password, client_key, client_secret))
    return 200

def move(oauth, item_id, is_folder=False):
    logging.debug( "DELETE %s %s %s" %(oauth, item_id, is_folder))
    return 200
